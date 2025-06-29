/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.service;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.LocalNodeMasterListener;
import org.elasticsearch.cluster.NodeConnectionsService;
import org.elasticsearch.cluster.TimeoutClusterStateListener;
import org.elasticsearch.cluster.metadata.ProcessClusterEventTimeoutException;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.common.util.concurrent.PrioritizedEsThreadPoolExecutor;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.elasticsearch.cluster.service.ClusterService.CLUSTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING;
import static org.elasticsearch.common.util.concurrent.EsExecutors.daemonThreadFactory;

public class ClusterApplierService extends AbstractLifecycleComponent implements ClusterApplier { // NOTE: htt, 应用集群状态, clusterStateChanged applier on cluster of current node

    public static final String CLUSTER_UPDATE_THREAD_NAME = "clusterApplierService#updateTask";

    private final ClusterSettings clusterSettings; // NOTE: htt, 集群配置
    protected final ThreadPool threadPool;

    private volatile TimeValue slowTaskLoggingThreshold;

    private volatile PrioritizedEsThreadPoolExecutor threadPoolExecutor; // NOTE: htt, threadPool executor，单线程执行，可以保证串行执行

    /**
     * Those 3 state listeners are changing infrequently - CopyOnWriteArrayList is just fine
     */
    private final Collection<ClusterStateApplier> highPriorityStateAppliers = new CopyOnWriteArrayList<>(); // NOTE: htt, high priority state appliers，目前为 IndicesClusterStateService
    private final Collection<ClusterStateApplier> normalPriorityStateAppliers = new CopyOnWriteArrayList<>(); // NOTE: htt, normal priority state appliers
    private final Collection<ClusterStateApplier> lowPriorityStateAppliers = new CopyOnWriteArrayList<>(); // NOTE: htt, htt priority state appliers
    private final Iterable<ClusterStateApplier> clusterStateAppliers = Iterables.concat(highPriorityStateAppliers,
        normalPriorityStateAppliers, lowPriorityStateAppliers); // NOTE:htt, 集群状态变化后，会按高、normal、低优先级 applier执行集群状态

    private final Collection<ClusterStateListener> clusterStateListeners = new CopyOnWriteArrayList<>(); // NOTE: htt, clusterState listener
    private final Collection<TimeoutClusterStateListener> timeoutClusterStateListeners =
        Collections.newSetFromMap(new ConcurrentHashMap<TimeoutClusterStateListener, Boolean>()); // NOTE: htt, timeout cluster state listener

    private final LocalNodeMasterListeners localNodeMasterListeners;

    private final Queue<NotifyTimeout> onGoingTimeouts = ConcurrentCollections.newQueue(); // NOTE: htt, including TimeoutClusterStateListener and timeout

    private final AtomicReference<ClusterState> state; // last applied state // NOTE: htt, 最后应用的cluster state

    private NodeConnectionsService nodeConnectionsService; // NOTE: htt, node connection service
    private Supplier<ClusterState.Builder> stateBuilderSupplier; // NOTE: htt, state builder

    public ClusterApplierService(Settings settings, ClusterSettings clusterSettings, ThreadPool threadPool, Supplier<ClusterState
        .Builder> stateBuilderSupplier) {
        super(settings);
        this.clusterSettings = clusterSettings;
        this.threadPool = threadPool;
        this.state = new AtomicReference<>();
        this.slowTaskLoggingThreshold = CLUSTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING.get(settings);
        this.localNodeMasterListeners = new LocalNodeMasterListeners(threadPool);
        this.stateBuilderSupplier = stateBuilderSupplier;
    }

    public void setSlowTaskLoggingThreshold(TimeValue slowTaskLoggingThreshold) {
        this.slowTaskLoggingThreshold = slowTaskLoggingThreshold;
    }

    public synchronized void setNodeConnectionsService(NodeConnectionsService nodeConnectionsService) {
        assert this.nodeConnectionsService == null : "nodeConnectionsService is already set";
        this.nodeConnectionsService = nodeConnectionsService;
    }

    @Override
    public void setInitialState(ClusterState initialState) {
        if (lifecycle.started()) {
            throw new IllegalStateException("can't set initial state when started");
        }
        assert state.get() == null : "state is already set";
        state.set(initialState);
    }

    @Override
    protected synchronized void doStart() { // NOTE: htt, start which add listener and create threadPool
        Objects.requireNonNull(nodeConnectionsService, "please set the node connection service before starting");
        Objects.requireNonNull(state.get(), "please set initial state before starting");
        addListener(localNodeMasterListeners);
        threadPoolExecutor = EsExecutors.newSinglePrioritizing( // NOTE: htt, create priority queue thread pool，单线程优先级高处理
                nodeName() + "/" + CLUSTER_UPDATE_THREAD_NAME,
                daemonThreadFactory(settings, CLUSTER_UPDATE_THREAD_NAME),
                threadPool.getThreadContext(),
                threadPool.scheduler());
    }

    class UpdateTask extends SourcePrioritizedRunnable implements Function<ClusterState, ClusterState> { // NOTE: htt, update task which apply oldClusterState to newClusterState
        final ClusterStateTaskListener listener;
        final Function<ClusterState, ClusterState> updateFunction;

        UpdateTask(Priority priority, String source, ClusterStateTaskListener listener,
                   Function<ClusterState, ClusterState> updateFunction) {
            super(priority, source);
            this.listener = listener;
            this.updateFunction = updateFunction;
        }

        @Override
        public ClusterState apply(ClusterState clusterState) {
            return updateFunction.apply(clusterState); // NOTE: htt, apply clusterState
        }

        @Override
        public void run() {
            runTask(this); // NOTE: htt, runTask
        }
    }

    @Override
    protected synchronized void doStop() { // NOTE: htt, stop service
        for (NotifyTimeout onGoingTimeout : onGoingTimeouts) {
            onGoingTimeout.cancel();
            try {
                onGoingTimeout.cancel();
                onGoingTimeout.listener.onClose();
            } catch (Exception ex) {
                logger.debug("failed to notify listeners on shutdown", ex);
            }
        }
        ThreadPool.terminate(threadPoolExecutor, 10, TimeUnit.SECONDS); // NOTE: htt, terminate threadPoolExecutor and wait 10 seconds
        // close timeout listeners that did not have an ongoing timeout
        timeoutClusterStateListeners.forEach(TimeoutClusterStateListener::onClose);
        removeListener(localNodeMasterListeners);
    }

    @Override
    protected synchronized void doClose() {
    }

    /**
     * The current cluster state.
     * Should be renamed to appliedClusterState
     */
    public ClusterState state() {
        assert assertNotCalledFromClusterStateApplier("the applied cluster state is not yet available");
        ClusterState clusterState = this.state.get();
        assert clusterState != null : "initial cluster state not set yet";
        return clusterState;
    }

    /**
     * Adds a high priority applier of updated cluster states.
     */
    public void addHighPriorityApplier(ClusterStateApplier applier) {
        highPriorityStateAppliers.add(applier); // NOTE:htt, 添加高优先级applier(IndicesClusterStateService)，在集群状态变化后执行
    }

    /**
     * Adds an applier which will be called after all high priority and normal appliers have been called.
     */
    public void addLowPriorityApplier(ClusterStateApplier applier) {
        lowPriorityStateAppliers.add(applier);
    }

    /**
     * Adds a applier of updated cluster states.
     */
    public void addStateApplier(ClusterStateApplier applier) {
        normalPriorityStateAppliers.add(applier);
    }

    /**
     * Removes an applier of updated cluster states.
     */
    public void removeApplier(ClusterStateApplier applier) {
        normalPriorityStateAppliers.remove(applier);
        highPriorityStateAppliers.remove(applier);
        lowPriorityStateAppliers.remove(applier);
    }

    /**
     * Add a listener for updated cluster states
     */
    public void addListener(ClusterStateListener listener) {
        clusterStateListeners.add(listener);
    }

    /**
     * Removes a listener for updated cluster states.
     */
    public void removeListener(ClusterStateListener listener) {
        clusterStateListeners.remove(listener);
    }

    /**
     * Removes a timeout listener for updated cluster states.
     */
    public void removeTimeoutListener(TimeoutClusterStateListener listener) {
        timeoutClusterStateListeners.remove(listener);
        for (Iterator<NotifyTimeout> it = onGoingTimeouts.iterator(); it.hasNext(); ) { // NOTE: htt, remove listener in onGogingTimeouts
            NotifyTimeout timeout = it.next();
            if (timeout.listener.equals(listener)) {
                timeout.cancel();
                it.remove();
            }
        }
    }

    /**
     * Add a listener for on/off local node master events
     */
    public void addLocalNodeMasterListener(LocalNodeMasterListener listener) {
        localNodeMasterListeners.add(listener);
    }

    /**
     * Remove the given listener for on/off local master events
     */
    public void removeLocalNodeMasterListener(LocalNodeMasterListener listener) {
        localNodeMasterListeners.remove(listener);
    }

    /**
     * Adds a cluster state listener that is expected to be removed during a short period of time.
     * If provided, the listener will be notified once a specific time has elapsed.
     *
     * NOTE: the listener is not removed on timeout. This is the responsibility of the caller.
     */
    public void addTimeoutListener(@Nullable final TimeValue timeout, final TimeoutClusterStateListener listener) {
        if (lifecycle.stoppedOrClosed()) {
            listener.onClose();
            return;
        }
        // call the post added notification on the same event thread
        try {
            threadPoolExecutor.execute(new SourcePrioritizedRunnable(Priority.HIGH, "_add_listener_") { // NOTE: htt, 添加指定时间后超时的处理任务
                @Override
                public void run() {
                    if (timeout != null) {
                        NotifyTimeout notifyTimeout = new NotifyTimeout(listener, timeout);
                        notifyTimeout.future = threadPool.schedule(timeout, ThreadPool.Names.GENERIC, notifyTimeout); // NOTE: htt, 指定timeout时间后执行
                        onGoingTimeouts.add(notifyTimeout);
                    }
                    timeoutClusterStateListeners.add(listener); // NOTE: htt, add listener to timeoutClusterState
                    listener.postAdded();
                }
            });
        } catch (EsRejectedExecutionException e) {
            if (lifecycle.stoppedOrClosed()) {
                listener.onClose();
            } else {
                throw e;
            }
        }
    }

    public void runOnApplierThread(final String source, Consumer<ClusterState> clusterStateConsumer,
                                   final ClusterStateTaskListener listener, Priority priority) {
        submitStateUpdateTask(source, ClusterStateTaskConfig.build(priority),
            (clusterState) -> {
                clusterStateConsumer.accept(clusterState);
                return clusterState;
            },
            listener);
    }

    public void runOnApplierThread(final String source, Consumer<ClusterState> clusterStateConsumer,
                                   final ClusterStateTaskListener listener) {
        runOnApplierThread(source, clusterStateConsumer, listener, Priority.HIGH); // NOTE: htt, priority is High for clusterStateChanged
    }

    @Override
    public void onNewClusterState(final String source, final Supplier<ClusterState> clusterStateSupplier,
                                  final ClusterStateTaskListener listener) { // NOTE: htt, new clusterState then update
        Function<ClusterState, ClusterState> applyFunction = currentState -> {
            ClusterState nextState = clusterStateSupplier.get();
            if (nextState != null) {
                return nextState;
            } else {
                return currentState;
            }
        };
        submitStateUpdateTask(source, ClusterStateTaskConfig.build(Priority.HIGH), applyFunction, listener);
    }

    private void submitStateUpdateTask(final String source, final ClusterStateTaskConfig config,
                                       final Function<ClusterState, ClusterState> executor,
                                       final ClusterStateTaskListener listener) {
        if (!lifecycle.started()) {
            return;
        }
        try {
            UpdateTask updateTask = new UpdateTask(config.priority(), source, new SafeClusterStateTaskListener(listener, logger), executor);
            if (config.timeout() != null) {
                threadPoolExecutor.execute(updateTask, config.timeout(), // NOTE: htt, excute with timeout
                    () -> threadPool.generic().execute(
                        () -> listener.onFailure(source, new ProcessClusterEventTimeoutException(config.timeout(), source))));
            } else {
                threadPoolExecutor.execute(updateTask);
            }
        } catch (EsRejectedExecutionException e) {
            // ignore cases where we are shutting down..., there is really nothing interesting
            // to be done here...
            if (!lifecycle.stoppedOrClosed()) {
                throw e;
            }
        }
    }

    /** asserts that the current thread is the cluster state update thread */
    public static boolean assertClusterStateUpdateThread() {
        assert Thread.currentThread().getName().contains(ClusterApplierService.CLUSTER_UPDATE_THREAD_NAME) :
            "not called from the cluster state update thread";
        return true;
    }

    /** asserts that the current thread is <b>NOT</b> the cluster state update thread */
    public static boolean assertNotClusterStateUpdateThread(String reason) {
        assert Thread.currentThread().getName().contains(CLUSTER_UPDATE_THREAD_NAME) == false :
            "Expected current thread [" + Thread.currentThread() + "] to not be the cluster state update thread. Reason: [" + reason + "]";
        return true;
    }

    /** asserts that the current stack trace does <b>NOT</b> involve a cluster state applier */
    private static boolean assertNotCalledFromClusterStateApplier(String reason) {
        if (Thread.currentThread().getName().contains(CLUSTER_UPDATE_THREAD_NAME)) {
            for (StackTraceElement element : Thread.currentThread().getStackTrace()) {
                final String className = element.getClassName();
                final String methodName = element.getMethodName();
                if (className.equals(ClusterStateObserver.class.getName())) {
                    // people may start an observer from an applier
                    return true;
                } else if (className.equals(ClusterApplierService.class.getName())
                    && methodName.equals("callClusterStateAppliers")) {
                    throw new AssertionError("should not be called by a cluster state applier. reason [" + reason + "]");
                }
            }
        }
        return true;
    }

    protected void runTask(UpdateTask task) { // NOTE: htt, run task of clusterStateChanged，执行集群状态变化的
        if (!lifecycle.started()) {
            logger.debug("processing [{}]: ignoring, cluster applier service not started", task.source);
            return;
        }

        logger.debug("processing [{}]: execute", task.source);
        final ClusterState previousClusterState = state.get();

        long startTimeNS = currentTimeInNanos();
        final ClusterState newClusterState;
        try {
            newClusterState = task.apply(previousClusterState);
        } catch (Exception e) {
            TimeValue executionTime = TimeValue.timeValueMillis(Math.max(0, TimeValue.nsecToMSec(currentTimeInNanos() - startTimeNS)));
            if (logger.isTraceEnabled()) {
                logger.trace(() -> new ParameterizedMessage(
                        "failed to execute cluster state applier in [{}], state:\nversion [{}], source [{}]\n{}{}{}",
                        executionTime,
                        previousClusterState.version(),
                        task.source,
                        previousClusterState.nodes(),
                        previousClusterState.routingTable(),
                        previousClusterState.getRoutingNodes()),
                    e);
            }
            warnAboutSlowTaskIfNeeded(executionTime, task.source);
            task.listener.onFailure(task.source, e);
            return;
        }

        if (previousClusterState == newClusterState) {
            task.listener.clusterStateProcessed(task.source, newClusterState, newClusterState);
            TimeValue executionTime = TimeValue.timeValueMillis(Math.max(0, TimeValue.nsecToMSec(currentTimeInNanos() - startTimeNS)));
            logger.debug("processing [{}]: took [{}] no change in cluster state", task.source, executionTime);
            warnAboutSlowTaskIfNeeded(executionTime, task.source);
        } else {
            if (logger.isTraceEnabled()) {
                logger.trace("cluster state updated, source [{}]\n{}", task.source, newClusterState);
            } else if (logger.isDebugEnabled()) {
                logger.debug("cluster state updated, version [{}], source [{}]", newClusterState.version(), task.source);
            }
            try {
                applyChanges(task, previousClusterState, newClusterState); // NOTE: htt, 应用集群状态
                TimeValue executionTime = TimeValue.timeValueMillis(Math.max(0, TimeValue.nsecToMSec(currentTimeInNanos() - startTimeNS)));
                logger.debug("processing [{}]: took [{}] done applying updated cluster state (version: {}, uuid: {})", task.source,
                    executionTime, newClusterState.version(),
                    newClusterState.stateUUID());
                warnAboutSlowTaskIfNeeded(executionTime, task.source);
            } catch (Exception e) {
                TimeValue executionTime = TimeValue.timeValueMillis(Math.max(0, TimeValue.nsecToMSec(currentTimeInNanos() - startTimeNS)));
                final long version = newClusterState.version();
                final String stateUUID = newClusterState.stateUUID();
                final String fullState = newClusterState.toString();
                logger.warn(() -> new ParameterizedMessage(
                        "failed to apply updated cluster state in [{}]:\nversion [{}], uuid [{}], source [{}]\n{}",
                        executionTime,
                        version,
                        stateUUID,
                        task.source,
                        fullState),
                    e);
                // TODO: do we want to call updateTask.onFailure here?
            }
        }
    }

    private void applyChanges(UpdateTask task, ClusterState previousClusterState, ClusterState newClusterState) { // NOTE: htt, apply clusterState change on current node
        ClusterChangedEvent clusterChangedEvent = new ClusterChangedEvent(task.source, newClusterState, previousClusterState);
        // new cluster state, notify all listeners
        final DiscoveryNodes.Delta nodesDelta = clusterChangedEvent.nodesDelta();
        if (nodesDelta.hasChanges() && logger.isInfoEnabled()) {
            String summary = nodesDelta.shortSummary();
            if (summary.length() > 0) {
                logger.info("{}, reason: {}", summary, task.source);
            }
        }

        nodeConnectionsService.connectToNodes(newClusterState.nodes()); // NOTE: htt, 在收到集群新的状态后就会重新建立连接, 连接到新节点， connect to nodes of new clusterState

        logger.debug("applying cluster state version {}", newClusterState.version());
        try {
            // nothing to do until we actually recover from the gateway or any other block indicates we need to disable persistency
            if (clusterChangedEvent.state().blocks().disableStatePersistence() == false && clusterChangedEvent.metaDataChanged()) {
                final Settings incomingSettings = clusterChangedEvent.state().metaData().settings();
                clusterSettings.applySettings(incomingSettings); // NOTE: htt, 应用集群配置
            }
        } catch (Exception ex) {
            logger.warn("failed to apply cluster settings", ex);
        }

        logger.debug("apply cluster state with version {}", newClusterState.version());
        callClusterStateAppliers(clusterChangedEvent); // NOTE: htt, 应用集群状态，包括 IndicesClusterStateService，更新集群索引

        nodeConnectionsService.disconnectFromNodesExcept(newClusterState.nodes()); // NOTE: htt, 断开老节点, disconnect from nodes which are not in newClusterState.nodes

        logger.debug("set locally applied cluster state to version {}", newClusterState.version());
        state.set(newClusterState); // NOTE: htt, update local clusterState

        callClusterStateListeners(clusterChangedEvent);

        task.listener.clusterStateProcessed(task.source, previousClusterState, newClusterState); // NOTE: htt, task listener process clusterState changed
    }

    private void callClusterStateAppliers(ClusterChangedEvent clusterChangedEvent) { // NOTE:htt, 应用集群状态
        clusterStateAppliers.forEach(applier -> {
            try {
                logger.trace("calling [{}] with change to version [{}]", applier, clusterChangedEvent.state().version());
                applier.applyClusterState(clusterChangedEvent); // NOTE: htt, apply new cluster state changed
            } catch (Exception ex) {
                logger.warn("failed to notify ClusterStateApplier", ex);
            }
        });
    }

    private void callClusterStateListeners(ClusterChangedEvent clusterChangedEvent) {
        Stream.concat(clusterStateListeners.stream(), timeoutClusterStateListeners.stream()).forEach(listener -> {
            try {
                logger.trace("calling [{}] with change to version [{}]", listener, clusterChangedEvent.state().version());
                listener.clusterChanged(clusterChangedEvent);
            } catch (Exception ex) {
                logger.warn("failed to notify ClusterStateListener", ex);
            }
        });
    }

    private static class SafeClusterStateTaskListener implements ClusterStateTaskListener { // NOTE: htt, safe cluster state task listener
        private final ClusterStateTaskListener listener;
        private final Logger logger;

        SafeClusterStateTaskListener(ClusterStateTaskListener listener, Logger logger) {
            this.listener = listener;
            this.logger = logger;
        }

        @Override
        public void onFailure(String source, Exception e) {
            try {
                listener.onFailure(source, e);
            } catch (Exception inner) {
                inner.addSuppressed(e);
                logger.error(new ParameterizedMessage(
                        "exception thrown by listener notifying of failure from [{}]", source), inner);
            }
        }

        @Override
        public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
            try {
                listener.clusterStateProcessed(source, oldState, newState);
            } catch (Exception e) {
                logger.error(new ParameterizedMessage(
                        "exception thrown by listener while notifying of cluster state processed from [{}], old cluster state:\n" +
                            "{}\nnew cluster state:\n{}",
                        source, oldState, newState), e);
            }
        }
    }

    protected void warnAboutSlowTaskIfNeeded(TimeValue executionTime, String source) {
        if (executionTime.getMillis() > slowTaskLoggingThreshold.getMillis()) {
            logger.warn("cluster state applier task [{}] took [{}] above the warn threshold of {}", source, executionTime,
                slowTaskLoggingThreshold);
        }
    }

    class NotifyTimeout implements Runnable { // NOTE: htt, notifyTimeout runnable
        final TimeoutClusterStateListener listener; // NOTE: htt, ObserverClusterStateListener which observer cluster state
        final TimeValue timeout;
        volatile ScheduledFuture future; // NOTE: htt, 调度任务

        NotifyTimeout(TimeoutClusterStateListener listener, TimeValue timeout) {
            this.listener = listener;
            this.timeout = timeout;
        }

        public void cancel() {
            FutureUtils.cancel(future);
        }

        @Override
        public void run() {
            if (future != null && future.isCancelled()) {
                return;
            }
            if (lifecycle.stoppedOrClosed()) {
                listener.onClose();
            } else {
                listener.onTimeout(this.timeout); // NOTE: htt, ObserverClusterStateListener.onTimeout()，超时情况
            }
            // note, we rely on the listener to remove itself in case of timeout if needed
        }
    }

    private static class LocalNodeMasterListeners implements ClusterStateListener { // NOTE: htt, localNode on master listeners : when is onMaster/offMaster to executor listener

        private final List<LocalNodeMasterListener> listeners = new CopyOnWriteArrayList<>(); // NOTE: htt, localNodeMasterListener
        private final ThreadPool threadPool;
        private volatile boolean master = false;

        private LocalNodeMasterListeners(ThreadPool threadPool) {
            this.threadPool = threadPool;
        }

        @Override
        public void clusterChanged(ClusterChangedEvent event) {
            if (!master && event.localNodeMaster()) {
                master = true;
                for (LocalNodeMasterListener listener : listeners) {
                    java.util.concurrent.Executor executor = threadPool.executor(listener.executorName());
                    executor.execute(new OnMasterRunnable(listener));
                }
                return;
            }

            if (master && !event.localNodeMaster()) {
                master = false;
                for (LocalNodeMasterListener listener : listeners) {
                    java.util.concurrent.Executor executor = threadPool.executor(listener.executorName());
                    executor.execute(new OffMasterRunnable(listener));
                }
            }
        }

        private void add(LocalNodeMasterListener listener) {
            listeners.add(listener);
        }

        private void remove(LocalNodeMasterListener listener) {
            listeners.remove(listener);
        }

        private void clear() {
            listeners.clear();
        }
    }

    private static class OnMasterRunnable implements Runnable { // NOTE: htt, onMaster runnable

        private final LocalNodeMasterListener listener;

        private OnMasterRunnable(LocalNodeMasterListener listener) {
            this.listener = listener;
        }

        @Override
        public void run() {
            listener.onMaster();
        }
    }

    private static class OffMasterRunnable implements Runnable { // NOTE: htt, offMaster runnable

        private final LocalNodeMasterListener listener;

        private OffMasterRunnable(LocalNodeMasterListener listener) {
            this.listener = listener;
        }

        @Override
        public void run() {
            listener.offMaster();
        }
    }

    // this one is overridden in tests so we can control time
    protected long currentTimeInNanos() {
        return System.nanoTime();
    }

    @Override
    public ClusterState.Builder newClusterStateBuilder() {
        return stateBuilderSupplier.get();
    }
}
