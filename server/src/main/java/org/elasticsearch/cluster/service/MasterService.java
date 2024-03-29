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
import org.elasticsearch.Assertions;
import org.elasticsearch.cluster.AckedClusterStateTaskListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterState.Builder;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskExecutor.ClusterTasksResult;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.ProcessClusterEventTimeoutException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.common.util.concurrent.PrioritizedEsThreadPoolExecutor;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.service.ClusterService.CLUSTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING;
import static org.elasticsearch.common.util.concurrent.EsExecutors.daemonThreadFactory;

public class MasterService extends AbstractLifecycleComponent { // NOTE: htt, masterService execute batch tasks and updateClusterState is has been changed

    public static final String MASTER_UPDATE_THREAD_NAME = "masterService#updateTask";

    private BiConsumer<ClusterChangedEvent, Discovery.AckListener> clusterStatePublisher; // NOTE:htt, ZenDiscovery::publish

    private java.util.function.Supplier<ClusterState> clusterStateSupplier;

    private volatile TimeValue slowTaskLoggingThreshold;

    protected final ThreadPool threadPool;

    private volatile PrioritizedEsThreadPoolExecutor threadPoolExecutor; // NOTE: htt, 单线程
    private volatile Batcher taskBatcher;

    public MasterService(Settings settings, ThreadPool threadPool) {
        super(settings);
        // TODO: introduce a dedicated setting for master service
        this.slowTaskLoggingThreshold = CLUSTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING.get(settings);
        this.threadPool = threadPool;
    }

    public void setSlowTaskLoggingThreshold(TimeValue slowTaskLoggingThreshold) {
        this.slowTaskLoggingThreshold = slowTaskLoggingThreshold;
    }

    public synchronized void setClusterStatePublisher(BiConsumer<ClusterChangedEvent, Discovery.AckListener> publisher) {
        clusterStatePublisher = publisher;
    }

    public synchronized void setClusterStateSupplier(java.util.function.Supplier<ClusterState> clusterStateSupplier) {
        this.clusterStateSupplier = clusterStateSupplier;
    }

    @Override
    protected synchronized void doStart() { // NOTE: htt, doStart
        Objects.requireNonNull(clusterStatePublisher, "please set a cluster state publisher before starting");
        Objects.requireNonNull(clusterStateSupplier, "please set a cluster state supplier before starting");
        threadPoolExecutor = EsExecutors.newSinglePrioritizing( // NOTE: htt, 单线程, create prioritizing threadPool
                nodeName() + "/" + MASTER_UPDATE_THREAD_NAME,
                daemonThreadFactory(settings, MASTER_UPDATE_THREAD_NAME),
                threadPool.getThreadContext(),
                threadPool.scheduler());
        taskBatcher = new Batcher(logger, threadPoolExecutor);
    }

    class Batcher extends TaskBatcher { // NOTE:  htt, batcher to run tasks

        Batcher(Logger logger, PrioritizedEsThreadPoolExecutor threadExecutor) {
            super(logger, threadExecutor);
        }

        @Override
        protected void onTimeout(List<? extends BatchedTask> tasks, TimeValue timeout) {
            threadPool.generic().execute(
                () -> tasks.forEach(
                    task -> ((UpdateTask) task).listener.onFailure(task.source,
                        new ProcessClusterEventTimeoutException(timeout, task.source))));
        }

        @Override
        protected void run(Object batchingKey, List<? extends BatchedTask> tasks, String tasksSummary) {
            ClusterStateTaskExecutor<Object> taskExecutor = (ClusterStateTaskExecutor<Object>) batchingKey;
            List<UpdateTask> updateTasks = (List<UpdateTask>) tasks;
            runTasks(new TaskInputs(taskExecutor, updateTasks, tasksSummary));
        }

        class UpdateTask extends BatchedTask { // NOTE: htt, updateTask
            final ClusterStateTaskListener listener;

            UpdateTask(Priority priority, String source, Object task, ClusterStateTaskListener listener,
                       ClusterStateTaskExecutor<?> executor) {
                super(priority, source, executor, task);
                this.listener = listener;
            }

            @Override
            public String describeTasks(List<? extends BatchedTask> tasks) {
                return ((ClusterStateTaskExecutor<Object>) batchingKey).describeTasks(
                    tasks.stream().map(BatchedTask::getTask).collect(Collectors.toList()));
            }
        }
    }

    @Override
    protected synchronized void doStop() {
        ThreadPool.terminate(threadPoolExecutor, 10, TimeUnit.SECONDS);
    }

    @Override
    protected synchronized void doClose() {
    }

    /**
     * The current cluster state exposed by the discovery layer. Package-visible for tests.
     */
    ClusterState state() {
        return clusterStateSupplier.get();
    }

    public static boolean assertMasterUpdateThread() {
        assert Thread.currentThread().getName().contains(MASTER_UPDATE_THREAD_NAME) :
            "not called from the master service thread";
        return true;
    }

    public static boolean assertNotMasterUpdateThread(String reason) {
        assert Thread.currentThread().getName().contains(MASTER_UPDATE_THREAD_NAME) == false :
            "Expected current thread [" + Thread.currentThread() + "] to not be the master service thread. Reason: [" + reason + "]";
        return true;
    }

    protected void runTasks(TaskInputs taskInputs) { // NOTE: htt, runTasks and publish clusterState if has been changed
        final String summary = taskInputs.summary;
        if (!lifecycle.started()) {
            logger.debug("processing [{}]: ignoring, master service not started", summary);
            return;
        }

        logger.debug("processing [{}]: execute", summary);
        final ClusterState previousClusterState = state();

        if (!previousClusterState.nodes().isLocalNodeElectedMaster() && taskInputs.runOnlyWhenMaster()) { // NOTE: htt, 当前节点不是master，但是需要在master上执行则按master异常处理
            logger.debug("failing [{}]: local node is no longer master", summary);
            taskInputs.onNoLongerMaster(); // NOTE:htt, 当前不在是master
            return;
        }

        long startTimeNS = currentTimeInNanos();
        TaskOutputs taskOutputs = calculateTaskOutputs(taskInputs, previousClusterState, startTimeNS); // NOTE: htt, 1. first run tasks
        taskOutputs.notifyFailedTasks();

        if (taskOutputs.clusterStateUnchanged()) {
            taskOutputs.notifySuccessfulTasksOnUnchangedClusterState(); // NOTE: htt, clusterState not changed
            TimeValue executionTime = TimeValue.timeValueMillis(Math.max(0, TimeValue.nsecToMSec(currentTimeInNanos() - startTimeNS)));
            logger.debug("processing [{}]: took [{}] no change in cluster state", summary, executionTime);
            warnAboutSlowTaskIfNeeded(executionTime, summary);
        } else { // NOTE: htt, update cluster state if has been changed，状态有更新，则更新集群状态
            ClusterState newClusterState = taskOutputs.newClusterState;
            if (logger.isTraceEnabled()) {
                logger.trace("cluster state updated, source [{}]\n{}", summary, newClusterState);
            } else if (logger.isDebugEnabled()) {
                logger.debug("cluster state updated, version [{}], source [{}]", newClusterState.version(), summary);
            }
            try {
                ClusterChangedEvent clusterChangedEvent = new ClusterChangedEvent(summary, newClusterState, previousClusterState);
                // new cluster state, notify all listeners
                final DiscoveryNodes.Delta nodesDelta = clusterChangedEvent.nodesDelta(); // NOTE: htt, get node change
                if (nodesDelta.hasChanges() && logger.isInfoEnabled()) {
                    String nodeSummary = nodesDelta.shortSummary();
                    if (nodeSummary.length() > 0) {
                        logger.info("{}, reason: {}", summary, nodeSummary); // NOTE: htt, get node changed
                    }
                }

                logger.debug("publishing cluster state version [{}]", newClusterState.version());
                try { // NOTE:htt, ZenDiscovery::publish， 将请求分发给集群节点，并等待结束？
                    clusterStatePublisher.accept(clusterChangedEvent, taskOutputs.createAckListener(threadPool, newClusterState));
                } catch (Discovery.FailedToCommitClusterStateException t) {
                    final long version = newClusterState.version();
                    logger.warn(() -> new ParameterizedMessage(
                            "failing [{}]: failed to commit cluster state version [{}]", summary, version), t);
                    taskOutputs.publishingFailed(t);
                    return;
                }

                taskOutputs.processedDifferentClusterState(previousClusterState, newClusterState); // NOTE: htt, 集群状态处理完毕后，执行listener的完成监听

                try {
                    taskOutputs.clusterStatePublished(clusterChangedEvent); // NOTE: htt, clusterState published
                } catch (Exception e) {
                    logger.error(() -> new ParameterizedMessage(
                            "exception thrown while notifying executor of new cluster state publication [{}]",
                            summary), e);
                }
                TimeValue executionTime = TimeValue.timeValueMillis(Math.max(0, TimeValue.nsecToMSec(currentTimeInNanos() - startTimeNS)));
                logger.debug("processing [{}]: took [{}] done publishing updated cluster state (version: {}, uuid: {})", summary,
                    executionTime, newClusterState.version(),
                    newClusterState.stateUUID());
                warnAboutSlowTaskIfNeeded(executionTime, summary);
            } catch (Exception e) {
                TimeValue executionTime = TimeValue.timeValueMillis(Math.max(0, TimeValue.nsecToMSec(currentTimeInNanos() - startTimeNS)));
                final long version = newClusterState.version();
                final String stateUUID = newClusterState.stateUUID();
                final String fullState = newClusterState.toString();
                logger.warn(() -> new ParameterizedMessage(
                        "failed to publish updated cluster state in [{}]:\nversion [{}], uuid [{}], source [{}]\n{}",
                        executionTime,
                        version,
                        stateUUID,
                        summary,
                        fullState),
                    e);
                // TODO: do we want to call updateTask.onFailure here?
            }
        }
    }

    public TaskOutputs calculateTaskOutputs(TaskInputs taskInputs, ClusterState previousClusterState, long startTimeNS) {
        ClusterTasksResult<Object> clusterTasksResult = executeTasks(taskInputs, startTimeNS, previousClusterState);
        ClusterState newClusterState = patchVersions(previousClusterState, clusterTasksResult); // NOTE:htt, 更新多个信息的版本
        return new TaskOutputs(taskInputs, previousClusterState, newClusterState, getNonFailedTasks(taskInputs, clusterTasksResult),
            clusterTasksResult.executionResults);
    }

    private ClusterState patchVersions(ClusterState previousClusterState, ClusterTasksResult<?> executionResult) { // NOTE:htt, 更新多个信息的版本
        ClusterState newClusterState = executionResult.resultingState;

        if (previousClusterState != newClusterState) {
            // only the master controls the version numbers
            Builder builder = ClusterState.builder(newClusterState).incrementVersion(); // NOTE: htt, 增加集群状态的版本号
            if (previousClusterState.routingTable() != newClusterState.routingTable()) { // NOTE;htt, 路由表如果不一致，则更新路由表版本
                builder.routingTable(RoutingTable.builder(newClusterState.routingTable())
                    .version(newClusterState.routingTable().version() + 1).build()); // NOTE: htt, update routingTable and include version if need
            }
            if (previousClusterState.metaData() != newClusterState.metaData()) { // NOTE:htt, 元数据如果不一致，则更新元数据版本
                builder.metaData(MetaData.builder(newClusterState.metaData()).version(newClusterState.metaData().version() + 1)); // NOTE: htt, update metaData and increament version if need
            }

            newClusterState = builder.build();
        }

        return newClusterState;
    }

    /**
     * Submits a cluster state update task; unlike {@link #submitStateUpdateTask(String, Object, ClusterStateTaskConfig,
     * ClusterStateTaskExecutor, ClusterStateTaskListener)}, submitted updates will not be batched.
     *
     * @param source     the source of the cluster state update task
     * @param updateTask the full context for the cluster state update
     *                   task
     *
     */
    public <T extends ClusterStateTaskConfig & ClusterStateTaskExecutor<T> & ClusterStateTaskListener>
        void submitStateUpdateTask(
        String source, T updateTask) {
        submitStateUpdateTask(source, updateTask, updateTask, updateTask, updateTask);
    }

    /**
     * Submits a cluster state update task; submitted updates will be
     * batched across the same instance of executor. The exact batching
     * semantics depend on the underlying implementation but a rough
     * guideline is that if the update task is submitted while there
     * are pending update tasks for the same executor, these update
     * tasks will all be executed on the executor in a single batch
     *
     * @param source   the source of the cluster state update task
     * @param task     the state needed for the cluster state update task
     * @param config   the cluster state update task configuration
     * @param executor the cluster state update task executor; tasks
     *                 that share the same executor will be executed
     *                 batches on this executor
     * @param listener callback after the cluster state update task
     *                 completes
     * @param <T>      the type of the cluster state update task state
     *
     */
    public <T> void submitStateUpdateTask(String source, T task,
                                          ClusterStateTaskConfig config,
                                          ClusterStateTaskExecutor<T> executor,
                                          ClusterStateTaskListener listener) {
        submitStateUpdateTasks(source, Collections.singletonMap(task, listener), config, executor);
    }

    /**
     * Output created by executing a set of tasks provided as TaskInputs
     */
    class TaskOutputs { // NOTE: htt, taskOutPuts with taskInputs and clusterState and nonFailedTasks
        public final TaskInputs taskInputs;
        public final ClusterState previousClusterState; // NOTE: htt, 原有集群状态
        public final ClusterState newClusterState; // NOTE: htt, 新的集群状态
        public final List<Batcher.UpdateTask> nonFailedTasks;
        public final Map<Object, ClusterStateTaskExecutor.TaskResult> executionResults;

        TaskOutputs(TaskInputs taskInputs, ClusterState previousClusterState,
                           ClusterState newClusterState,
                           List<Batcher.UpdateTask> nonFailedTasks,
                           Map<Object, ClusterStateTaskExecutor.TaskResult> executionResults) {
            this.taskInputs = taskInputs;
            this.previousClusterState = previousClusterState;
            this.newClusterState = newClusterState;
            this.nonFailedTasks = nonFailedTasks;
            this.executionResults = executionResults;
        }

        public void publishingFailed(Discovery.FailedToCommitClusterStateException t) { // NOTE: htt, 回滚，如果整个集群执行失败，那么nonFailedTasks就需要回滚
            nonFailedTasks.forEach(task -> task.listener.onFailure(task.source(), t));
        }

        public void processedDifferentClusterState(ClusterState previousClusterState, ClusterState newClusterState) { // NOTE: htt, nonFaieldTasks需要执行集群状态变化
            nonFailedTasks.forEach(task -> task.listener.clusterStateProcessed(task.source(), previousClusterState, newClusterState)); // NOTE:htt, 执行集群状态变化完成处理
        }

        public void clusterStatePublished(ClusterChangedEvent clusterChangedEvent) {
            taskInputs.executor.clusterStatePublished(clusterChangedEvent);
        }

        public Discovery.AckListener createAckListener(ThreadPool threadPool, ClusterState newClusterState) {
            ArrayList<Discovery.AckListener> ackListeners = new ArrayList<>();

            //timeout straightaway, otherwise we could wait forever as the timeout thread has not started
            nonFailedTasks.stream().filter(task -> task.listener instanceof AckedClusterStateTaskListener).forEach(task -> {
                final AckedClusterStateTaskListener ackedListener = (AckedClusterStateTaskListener) task.listener;
                if (ackedListener.ackTimeout() == null || ackedListener.ackTimeout().millis() == 0) {
                    ackedListener.onAckTimeout();
                } else {
                    try {
                        ackListeners.add(new AckCountDownListener(ackedListener, newClusterState.version(), newClusterState.nodes(),
                            threadPool));
                    } catch (EsRejectedExecutionException ex) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Couldn't schedule timeout thread - node might be shutting down", ex);
                        }
                        //timeout straightaway, otherwise we could wait forever as the timeout thread has not started
                        ackedListener.onAckTimeout();
                    }
                }
            });

            return new DelegetingAckListener(ackListeners);
        }

        public boolean clusterStateUnchanged() { // NOTE:htt, 新老集群状态一致
            return previousClusterState == newClusterState;
        }

        public void notifyFailedTasks() { // NOTE: htt, notifyFailedTasks
            // fail all tasks that have failed
            for (Batcher.UpdateTask updateTask : taskInputs.updateTasks) {
                assert executionResults.containsKey(updateTask.task) : "missing " + updateTask;
                final ClusterStateTaskExecutor.TaskResult taskResult = executionResults.get(updateTask.task);
                if (taskResult.isSuccess() == false) {
                    updateTask.listener.onFailure(updateTask.source(), taskResult.getFailure());
                }
            }
        }

        public void notifySuccessfulTasksOnUnchangedClusterState() {
            nonFailedTasks.forEach(task -> {
                if (task.listener instanceof AckedClusterStateTaskListener) {
                    //no need to wait for ack if nothing changed, the update can be counted as acknowledged
                    ((AckedClusterStateTaskListener) task.listener).onAllNodesAcked(null);
                }
                task.listener.clusterStateProcessed(task.source(), newClusterState, newClusterState); // NOTE:htt, 没有集群状态变更，则按集群状态成功处理
            });
        }
    }

    /**
     * Returns the tasks that are pending.
     */
    public List<PendingClusterTask> pendingTasks() { // NOTE: htt, pendingTasks
        return Arrays.stream(threadPoolExecutor.getPending()).map(pending -> {
            assert pending.task instanceof SourcePrioritizedRunnable :
                "thread pool executor should only use SourcePrioritizedRunnable instances but found: " + pending.task.getClass().getName();
            SourcePrioritizedRunnable task = (SourcePrioritizedRunnable) pending.task;
            return new PendingClusterTask(pending.insertionOrder, pending.priority, new Text(task.source()),
                task.getAgeInMillis(), pending.executing);
        }).collect(Collectors.toList());
    }

    /**
     * Returns the number of currently pending tasks.
     */
    public int numberOfPendingTasks() {
        return threadPoolExecutor.getNumberOfPendingTasks(); // NOTE: htt, number of pendingTasks
    }

    /**
     * Returns the maximum wait time for tasks in the queue
     *
     * @return A zero time value if the queue is empty, otherwise the time value oldest task waiting in the queue
     */
    public TimeValue getMaxTaskWaitTime() {
        return threadPoolExecutor.getMaxTaskWaitTime();
    }

    private SafeClusterStateTaskListener safe(ClusterStateTaskListener listener) {
        if (listener instanceof AckedClusterStateTaskListener) {
            return new SafeAckedClusterStateTaskListener((AckedClusterStateTaskListener) listener, logger);
        } else {
            return new SafeClusterStateTaskListener(listener, logger);
        }
    }

    private static class SafeClusterStateTaskListener implements ClusterStateTaskListener { // NOTE: htt, clusterStateTaskListener with logger
        private final ClusterStateTaskListener listener; // NOTE:htt, 集群状态任务监听
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
                logger.error(() -> new ParameterizedMessage(
                        "exception thrown by listener notifying of failure from [{}]", source), inner);
            }
        }

        @Override
        public void onNoLongerMaster(String source) {
            try {
                listener.onNoLongerMaster(source);
            } catch (Exception e) {
                logger.error(() -> new ParameterizedMessage(
                        "exception thrown by listener while notifying no longer master from [{}]", source), e);
            }
        }

        @Override
        public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) { // NOTE:htt, 集群状态变化
            try {
                listener.clusterStateProcessed(source, oldState, newState);
            } catch (Exception e) {
                logger.error(() -> new ParameterizedMessage(
                        "exception thrown by listener while notifying of cluster state processed from [{}], old cluster state:\n" +
                            "{}\nnew cluster state:\n{}", source, oldState, newState), e);
            }
        }
    }

    private static class SafeAckedClusterStateTaskListener extends SafeClusterStateTaskListener implements AckedClusterStateTaskListener { // NOTE: htt, safeAckedClusterStateTaskListener with logger
        private final AckedClusterStateTaskListener listener;
        private final Logger logger;

        SafeAckedClusterStateTaskListener(AckedClusterStateTaskListener listener, Logger logger) {
            super(listener, logger);
            this.listener = listener;
            this.logger = logger;
        }

        @Override
        public boolean mustAck(DiscoveryNode discoveryNode) {
            return listener.mustAck(discoveryNode);
        }

        @Override
        public void onAllNodesAcked(@Nullable Exception e) {
            try {
                listener.onAllNodesAcked(e);
            } catch (Exception inner) {
                inner.addSuppressed(e);
                logger.error("exception thrown by listener while notifying on all nodes acked", inner);
            }
        }

        @Override
        public void onAckTimeout() {
            try {
                listener.onAckTimeout();
            } catch (Exception e) {
                logger.error("exception thrown by listener while notifying on ack timeout", e);
            }
        }

        @Override
        public TimeValue ackTimeout() {
            return listener.ackTimeout();
        }
    }

    protected void warnAboutSlowTaskIfNeeded(TimeValue executionTime, String source) {
        if (executionTime.getMillis() > slowTaskLoggingThreshold.getMillis()) {
            logger.warn("cluster state update task [{}] took [{}] above the warn threshold of {}", source, executionTime,
                slowTaskLoggingThreshold);
        }
    }

    private static class DelegetingAckListener implements Discovery.AckListener { // NOTE: htt, AckListener代理

        private final List<Discovery.AckListener> listeners; // NOTE:htt, 一批AckListener

        private DelegetingAckListener(List<Discovery.AckListener> listeners) {
            this.listeners = listeners;
        }

        @Override
        public void onNodeAck(DiscoveryNode node, @Nullable Exception e) {
            for (Discovery.AckListener listener : listeners) {
                listener.onNodeAck(node, e);
            }
        }

        @Override
        public void onTimeout() {
            throw new UnsupportedOperationException("no timeout delegation");
        }
    }

    private static class AckCountDownListener implements Discovery.AckListener { // NOTE: htt, ackCountDownListener waiting mustAck nodes

        private static final Logger logger = Loggers.getLogger(AckCountDownListener.class);

        private final AckedClusterStateTaskListener ackedTaskListener;
        private final CountDown countDown;
        private final DiscoveryNodes nodes;
        private final long clusterStateVersion;
        private final Future<?> ackTimeoutCallback;
        private Exception lastFailure;

        AckCountDownListener(AckedClusterStateTaskListener ackedTaskListener, long clusterStateVersion, DiscoveryNodes nodes,
                             ThreadPool threadPool) {
            this.ackedTaskListener = ackedTaskListener;
            this.clusterStateVersion = clusterStateVersion;
            this.nodes = nodes;
            int countDown = 0;
            for (DiscoveryNode node : nodes) {
                if (ackedTaskListener.mustAck(node)) {
                    countDown++;
                }
            }
            //we always wait for at least 1 node (the master)
            countDown = Math.max(1, countDown);
            logger.trace("expecting {} acknowledgements for cluster_state update (version: {})", countDown, clusterStateVersion);
            this.countDown = new CountDown(countDown);
            this.ackTimeoutCallback = threadPool.schedule(ackedTaskListener.ackTimeout(), ThreadPool.Names.GENERIC, () -> onTimeout());
        }

        @Override
        public void onNodeAck(DiscoveryNode node, @Nullable Exception e) {
            if (!ackedTaskListener.mustAck(node)) {
                //we always wait for the master ack anyway
                if (!node.equals(nodes.getMasterNode())) {
                    return;
                }
            }
            if (e == null) {
                logger.trace("ack received from node [{}], cluster_state update (version: {})", node, clusterStateVersion);
            } else {
                this.lastFailure = e;
                logger.debug(() -> new ParameterizedMessage(
                        "ack received from node [{}], cluster_state update (version: {})", node, clusterStateVersion), e);
            }

            if (countDown.countDown()) { // NOTE: htt, has reached 0
                logger.trace("all expected nodes acknowledged cluster_state update (version: {})", clusterStateVersion);
                FutureUtils.cancel(ackTimeoutCallback);
                ackedTaskListener.onAllNodesAcked(lastFailure); // NOTE:htt, 执行所有节点完成ack后的处理
            }
        }

        @Override
        public void onTimeout() {
            if (countDown.fastForward()) {
                logger.trace("timeout waiting for acknowledgement for cluster_state update (version: {})", clusterStateVersion);
                ackedTaskListener.onAckTimeout();
            }
        }
    }

    protected ClusterTasksResult<Object> executeTasks(TaskInputs taskInputs, long startTimeNS, ClusterState previousClusterState) {
        ClusterTasksResult<Object> clusterTasksResult;
        try {
            List<Object> inputs = taskInputs.updateTasks.stream().map(tUpdateTask -> tUpdateTask.task).collect(Collectors.toList());
            clusterTasksResult = taskInputs.executor.execute(previousClusterState, inputs); // NOTE: htt, 执行对应的任务，如果出现异常则更新任务状态为失败
            if (previousClusterState != clusterTasksResult.resultingState &&
                previousClusterState.nodes().isLocalNodeElectedMaster() &&
                (clusterTasksResult.resultingState.nodes().isLocalNodeElectedMaster() == false)) { // NOTE:htt, 如果新的状态将本地节点设置为非master，则抛出异常
                throw new AssertionError("update task submitted to MasterService cannot remove master");
            }
        } catch (Exception e) {
            TimeValue executionTime = TimeValue.timeValueMillis(Math.max(0, TimeValue.nsecToMSec(currentTimeInNanos() - startTimeNS)));
            if (logger.isTraceEnabled()) {
                logger.trace(
                    () -> new ParameterizedMessage(
                        "failed to execute cluster state update in [{}], state:\nversion [{}], source [{}]\n{}{}{}",
                        executionTime,
                        previousClusterState.version(),
                        taskInputs.summary,
                        previousClusterState.nodes(),
                        previousClusterState.routingTable(),
                        previousClusterState.getRoutingNodes()),
                    e);
            }
            warnAboutSlowTaskIfNeeded(executionTime, taskInputs.summary);
            clusterTasksResult = ClusterTasksResult.builder()
                .failures(taskInputs.updateTasks.stream().map(updateTask -> updateTask.task)::iterator, e)
                .build(previousClusterState); // NOTE: htt, 任务执行异常，则失败的任务执行结果
        }

        assert clusterTasksResult.executionResults != null;
        assert clusterTasksResult.executionResults.size() == taskInputs.updateTasks.size()
            : String.format(Locale.ROOT, "expected [%d] task result%s but was [%d]", taskInputs.updateTasks.size(),
            taskInputs.updateTasks.size() == 1 ? "" : "s", clusterTasksResult.executionResults.size());
        if (Assertions.ENABLED) {
            ClusterTasksResult<Object> finalClusterTasksResult = clusterTasksResult;
            taskInputs.updateTasks.forEach(updateTask -> {
                assert finalClusterTasksResult.executionResults.containsKey(updateTask.task) :
                    "missing task result for " + updateTask;
            });
        }

        return clusterTasksResult;
    }

    public List<Batcher.UpdateTask> getNonFailedTasks(TaskInputs taskInputs,
                                                      ClusterTasksResult<Object> clusterTasksResult) { // NOTE:htt, 获取成功的任务
        return taskInputs.updateTasks.stream().filter(updateTask -> {
            assert clusterTasksResult.executionResults.containsKey(updateTask.task) : "missing " + updateTask;
            final ClusterStateTaskExecutor.TaskResult taskResult =
                clusterTasksResult.executionResults.get(updateTask.task);
            return taskResult.isSuccess(); // NOTE:htt, 执行成功的任务
        }).collect(Collectors.toList());
    }

    /**
     * Represents a set of tasks to be processed together with their executor
     */
    protected class TaskInputs { // NOTE: htt, represent a set of tasks with executor
        public final String summary;
        public final List<Batcher.UpdateTask> updateTasks; // NOTE:htt, 批量更新任务
        public final ClusterStateTaskExecutor<Object> executor; // NOTE:htt, 待执行的任务

        TaskInputs(ClusterStateTaskExecutor<Object> executor, List<Batcher.UpdateTask> updateTasks, String summary) {
            this.summary = summary;
            this.executor = executor;
            this.updateTasks = updateTasks;
        }

        public boolean runOnlyWhenMaster() {
            return executor.runOnlyOnMaster();
        }

        public void onNoLongerMaster() {
            updateTasks.forEach(task -> task.listener.onNoLongerMaster(task.source()));
        }
    }

    /**
     * Submits a batch of cluster state update tasks; submitted updates are guaranteed to be processed together,
     * potentially with more tasks of the same executor.
     *
     * @param source   the source of the cluster state update task
     * @param tasks    a map of update tasks and their corresponding listeners
     * @param config   the cluster state update task configuration
     * @param executor the cluster state update task executor; tasks
     *                 that share the same executor will be executed
     *                 batches on this executor
     * @param <T>      the type of the cluster state update task state
     *
     */
    public <T> void submitStateUpdateTasks(final String source,
                                           final Map<T, ClusterStateTaskListener> tasks, final ClusterStateTaskConfig config,
                                           final ClusterStateTaskExecutor<T> executor) {
        if (!lifecycle.started()) {
            return;
        }
        try {
            List<Batcher.UpdateTask> safeTasks = tasks.entrySet().stream()
                .map(e -> taskBatcher.new UpdateTask(config.priority(), source, e.getKey(), safe(e.getValue()), executor))
                .collect(Collectors.toList());
            taskBatcher.submitTasks(safeTasks, config.timeout()); // NOTE: htt, submit stateUpdateTasks, threadExecutor.executor() -> updateTask.run() -> runIfNotProcessed(updateTask) -> run(batchTasks) -> runTasks()
        } catch (EsRejectedExecutionException e) {
            // ignore cases where we are shutting down..., there is really nothing interesting
            // to be done here...
            if (!lifecycle.stoppedOrClosed()) {
                throw e;
            }
        }
    }

    // this one is overridden in tests so we can control time
    protected long currentTimeInNanos() {
        return System.nanoTime();
    }
}
