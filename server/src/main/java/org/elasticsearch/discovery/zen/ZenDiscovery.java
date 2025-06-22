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

package org.elasticsearch.discovery.zen;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterApplier;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.discovery.DiscoveryStats;
import org.elasticsearch.discovery.zen.PublishClusterStateAction.IncomingClusterStateListener;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.EmptyTransportResponseHandler;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;
import static org.elasticsearch.gateway.GatewayService.STATE_NOT_RECOVERED_BLOCK;

public class ZenDiscovery extends AbstractLifecycleComponent implements Discovery, PingContextProvider, IncomingClusterStateListener {

    public static final Setting<TimeValue> PING_TIMEOUT_SETTING =
        Setting.positiveTimeSetting("discovery.zen.ping_timeout", timeValueSeconds(3), Property.NodeScope); // NOTE: htt, ping 3 second
    public static final Setting<TimeValue> JOIN_TIMEOUT_SETTING =
        Setting.timeSetting("discovery.zen.join_timeout",
            settings -> TimeValue.timeValueMillis(PING_TIMEOUT_SETTING.get(settings).millis() * 20), // NOTE: htt, 加入master超时时间 is 60 seconds
            TimeValue.timeValueMillis(0), Property.NodeScope);
    public static final Setting<Integer> JOIN_RETRY_ATTEMPTS_SETTING =
        Setting.intSetting("discovery.zen.join_retry_attempts", 3, 1, Property.NodeScope); // NOTE: htt, join retry times is 3, not includint timeout
    public static final Setting<TimeValue> JOIN_RETRY_DELAY_SETTING =
        Setting.positiveTimeSetting("discovery.zen.join_retry_delay", TimeValue.timeValueMillis(100), Property.NodeScope); // NOTE: htt, join retry delay is 100ms
    public static final Setting<Integer> MAX_PINGS_FROM_ANOTHER_MASTER_SETTING =
        Setting.intSetting("discovery.zen.max_pings_from_another_master", 3, 1, Property.NodeScope); // NOTE:htt, 从其他maser节点ping最大次数
    public static final Setting<Boolean> SEND_LEAVE_REQUEST_SETTING =
        Setting.boolSetting("discovery.zen.send_leave_request", true, Property.NodeScope); // NOTE: htt, send leaver request，默认开启
    public static final Setting<TimeValue> MASTER_ELECTION_WAIT_FOR_JOINS_TIMEOUT_SETTING =
        Setting.timeSetting("discovery.zen.master_election.wait_for_joins_timeout",
            settings -> TimeValue.timeValueMillis(JOIN_TIMEOUT_SETTING.get(settings).millis() / 2), TimeValue.timeValueMillis(0), // NOTE: htt, master等待join timeout is 30s
            Property.NodeScope);
    public static final Setting<Boolean> MASTER_ELECTION_IGNORE_NON_MASTER_PINGS_SETTING =
            Setting.boolSetting("discovery.zen.master_election.ignore_non_master_pings", false, Property.NodeScope); // NOTE: htt, ignore non master pings, default false
    public static final Setting<Integer> MAX_PENDING_CLUSTER_STATES_SETTING =
        Setting.intSetting("discovery.zen.publish.max_pending_cluster_states", 25, 1, Property.NodeScope); // NOTE: htt, max pending cluster states is 25

    public static final String DISCOVERY_REJOIN_ACTION_NAME = "internal:discovery/zen/rejoin"; // NOTE:htt, rejion请求

    private final TransportService transportService;
    private final MasterService masterService;
    private final DiscoverySettings discoverySettings; // NOTE: htt, discovery settings including noMasterBlock(write block) and publish/commit timeout
    protected final ZenPing zenPing; // protected to allow tests access  // NOTE: htt, zen ping接口，包括pingCollection，其中包含一组节点和对应回包
    private final MasterFaultDetection masterFD; // NOTE; htt, 数据节点上执行master节点的存活探测,如果成功则1s后继续探测, 否则等待30s并重试3次，
    private final NodesFaultDetection nodesFD; // NOTE: htt,  在主master节点上的 fault 探测，探测的目标是其他所有的节点，如果存活则延迟1s继续探测，否则30s超时（重试3次后依旧则删除该节点）；如果是端口连接则立即删除节点（此时不重试）
    private final PublishClusterStateAction publishClusterState;
    private final MembershipAction membership; // NOTE:htt, 处理节点加入、验证、离开请求
    private final ThreadPool threadPool;

    private final TimeValue pingTimeout; // NOTE: htt, ping timeout is 3s
    private final TimeValue joinTimeout; // NOTE: htt, 加入master超时时间默认 is 60 seconds

    /** how many retry attempts to perform if join request failed with an retriable error */
    private final int joinRetryAttempts; // NOTE: htt, join retry times is 3, not including timeout
    /** how long to wait before performing another join attempt after a join request failed with an retriable error */
    private final TimeValue joinRetryDelay; // NOTE: htt, join retry delay is 100ms

    /** how many pings from *another* master to tolerate before forcing a rejoin on other or local master */
    private final int maxPingsFromAnotherMaster; // NOTE:htt, 从其他maser节点ping最大次数

    // a flag that should be used only for testing
    private final boolean sendLeaveRequest;  // NOTE: htt, send leaver request，默认开启

    private final ElectMasterService electMaster; // NOTE: htt, 候选节点个数满足要求，然后进行排序(先判断版本号(大的在前），若相等判断节点信息，id小的为在前)，再返回排序第一个候选节点作为master节点

    private final boolean masterElectionIgnoreNonMasters;  // NOTE: htt, ignore non master pings, default false
    private final TimeValue masterElectionWaitForJoinsTimeout;  // NOTE: htt, master等待join timeout，默认 is 30s

    private final JoinThreadControl joinThreadControl; // NOTE:htt, 控制join线程的启动、停止、标记完成等操作

    private final PendingClusterStatesQueue pendingStatesQueue;

    private final NodeJoinController nodeJoinController;
    private final NodeRemovalClusterStateTaskExecutor nodeRemovalExecutor;
    private final ClusterApplier clusterApplier;
    private final AtomicReference<ClusterState> committedState; // last committed cluster state // NOTE:htt, 最后提交的集群状态
    private final Object stateMutex = new Object(); // NOTE:htt, 状态互斥锁，用于同步对集群状态的访问
    private final Collection<BiConsumer<DiscoveryNode, ClusterState>> onJoinValidators;

    public ZenDiscovery(Settings settings, ThreadPool threadPool, TransportService transportService,
                        NamedWriteableRegistry namedWriteableRegistry, MasterService masterService, ClusterApplier clusterApplier,
                        ClusterSettings clusterSettings, UnicastHostsProvider hostsProvider, AllocationService allocationService,
                        Collection<BiConsumer<DiscoveryNode, ClusterState>> onJoinValidators) {
        super(settings);
        this.onJoinValidators = addBuiltInJoinValidators(onJoinValidators);
        this.masterService = masterService;
        this.clusterApplier = clusterApplier;
        this.transportService = transportService;
        this.discoverySettings = new DiscoverySettings(settings, clusterSettings);
        this.zenPing = newZenPing(settings, threadPool, transportService, hostsProvider);
        this.electMaster = new ElectMasterService(settings);
        this.pingTimeout = PING_TIMEOUT_SETTING.get(settings);
        this.joinTimeout = JOIN_TIMEOUT_SETTING.get(settings);
        this.joinRetryAttempts = JOIN_RETRY_ATTEMPTS_SETTING.get(settings);
        this.joinRetryDelay = JOIN_RETRY_DELAY_SETTING.get(settings);
        this.maxPingsFromAnotherMaster = MAX_PINGS_FROM_ANOTHER_MASTER_SETTING.get(settings);
        this.sendLeaveRequest = SEND_LEAVE_REQUEST_SETTING.get(settings);
        this.threadPool = threadPool;
        ClusterName clusterName = ClusterName.CLUSTER_NAME_SETTING.get(settings);
        this.committedState = new AtomicReference<>();

        this.masterElectionIgnoreNonMasters = MASTER_ELECTION_IGNORE_NON_MASTER_PINGS_SETTING.get(settings);
        this.masterElectionWaitForJoinsTimeout = MASTER_ELECTION_WAIT_FOR_JOINS_TIMEOUT_SETTING.get(settings);

        logger.debug("using ping_timeout [{}], join.timeout [{}], master_election.ignore_non_master [{}]",
                this.pingTimeout, joinTimeout, masterElectionIgnoreNonMasters);

        clusterSettings.addSettingsUpdateConsumer(ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING,
            this::handleMinimumMasterNodesChanged, (value) -> {
                final ClusterState clusterState = this.clusterState();
                int masterNodes = clusterState.nodes().getMasterNodes().size();
                // the purpose of this validation is to make sure that the master doesn't step down
                // due to a change in master nodes, which also means that there is no way to revert
                // an accidental change. Since we validate using the current cluster state (and
                // not the one from which the settings come from) we have to be careful and only
                // validate if the local node is already a master. Doing so all the time causes
                // subtle issues. For example, a node that joins a cluster has no nodes in its
                // current cluster state. When it receives a cluster state from the master with
                // a dynamic minimum master nodes setting int it, we must make sure we don't reject
                // it.

                if (clusterState.nodes().isLocalNodeElectedMaster() && value > masterNodes) {
                    throw new IllegalArgumentException("cannot set "
                        + ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING.getKey() + " to more than the current" +
                        " master nodes count [" + masterNodes + "]");
                }
        });

        this.masterFD = new MasterFaultDetection(settings, threadPool, transportService, this::clusterState, masterService, clusterName);
        this.masterFD.addListener(new MasterNodeFailureListener());
        this.nodesFD = new NodesFaultDetection(settings, threadPool, transportService, this::clusterState, clusterName);
        this.nodesFD.addListener(new NodeFaultDetectionListener());
        this.pendingStatesQueue = new PendingClusterStatesQueue(logger, MAX_PENDING_CLUSTER_STATES_SETTING.get(settings));

        this.publishClusterState =
                new PublishClusterStateAction(
                        settings,
                        transportService,
                        namedWriteableRegistry,
                        this,
                        discoverySettings);
        this.membership = new MembershipAction(settings, transportService, new MembershipListener(), onJoinValidators);
        this.joinThreadControl = new JoinThreadControl();

        this.nodeJoinController = new NodeJoinController(masterService, allocationService, electMaster, settings);
        this.nodeRemovalExecutor = new NodeRemovalClusterStateTaskExecutor(allocationService, electMaster, this::submitRejoin, logger);

        masterService.setClusterStateSupplier(this::clusterState);

        transportService.registerRequestHandler(
            DISCOVERY_REJOIN_ACTION_NAME, RejoinClusterRequest::new, ThreadPool.Names.SAME, new RejoinClusterRequestHandler());
    }

    static Collection<BiConsumer<DiscoveryNode,ClusterState>> addBuiltInJoinValidators(
        Collection<BiConsumer<DiscoveryNode,ClusterState>> onJoinValidators) {
        Collection<BiConsumer<DiscoveryNode, ClusterState>> validators = new ArrayList<>();
        validators.add((node, state) -> {
            MembershipAction.ensureNodesCompatibility(node.getVersion(), state.getNodes());
            MembershipAction.ensureIndexCompatibility(node.getVersion(), state.getMetaData());
        });
        validators.addAll(onJoinValidators);
        return Collections.unmodifiableCollection(validators);
    }

    // protected to allow overriding in tests
    protected ZenPing newZenPing(Settings settings, ThreadPool threadPool, TransportService transportService,
                                 UnicastHostsProvider hostsProvider) {
        return new UnicastZenPing(settings, threadPool, transportService, hostsProvider, this);
    }

    @Override
    protected void doStart() {
        DiscoveryNode localNode = transportService.getLocalNode();
        assert localNode != null;
        synchronized (stateMutex) {
            // set initial state
            assert committedState.get() == null;
            assert localNode != null;
            ClusterState.Builder builder = clusterApplier.newClusterStateBuilder();
            ClusterState initialState = builder
                .blocks(ClusterBlocks.builder()
                    .addGlobalBlock(STATE_NOT_RECOVERED_BLOCK)
                    .addGlobalBlock(discoverySettings.getNoMasterBlock()))
                .nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()))
                .build();
            committedState.set(initialState);
            clusterApplier.setInitialState(initialState);
            nodesFD.setLocalNode(localNode);
            joinThreadControl.start();
        }
        zenPing.start();
    }

    @Override
    public void startInitialJoin() {
        // start the join thread from a cluster state update. See {@link JoinThreadControl} for details.
        synchronized (stateMutex) {
            // do the join on a different thread, the caller of this method waits for 30s anyhow till it is discovered
            joinThreadControl.startNewThreadIfNotRunning();
        }
    }

    @Override
    protected void doStop() {
        joinThreadControl.stop();
        masterFD.stop("zen disco stop");
        nodesFD.stop();
        Releasables.close(zenPing); // stop any ongoing pinging
        DiscoveryNodes nodes = clusterState().nodes();
        if (sendLeaveRequest) {
            if (nodes.getMasterNode() == null) {
                // if we don't know who the master is, nothing to do here
            } else if (!nodes.isLocalNodeElectedMaster()) {
                try {
                    membership.sendLeaveRequestBlocking(nodes.getMasterNode(), nodes.getLocalNode(), TimeValue.timeValueSeconds(1));
                } catch (Exception e) {
                    logger.debug(() -> new ParameterizedMessage("failed to send leave request to master [{}]", nodes.getMasterNode()), e);
                }
            } else {
                // we're master -> let other potential master we left and start a master election now rather then wait for masterFD
                DiscoveryNode[] possibleMasters = electMaster.nextPossibleMasters(nodes.getNodes().values(), 5);
                for (DiscoveryNode possibleMaster : possibleMasters) {
                    if (nodes.getLocalNode().equals(possibleMaster)) {
                        continue;
                    }
                    try {
                        membership.sendLeaveRequest(nodes.getLocalNode(), possibleMaster);
                    } catch (Exception e) {
                        logger.debug(() -> new ParameterizedMessage("failed to send leave request from master [{}] to possible master [{}]", nodes.getMasterNode(), possibleMaster), e);
                    }
                }
            }
        }
    }

    @Override
    protected void doClose() throws IOException {
        IOUtils.close(masterFD, nodesFD);
    }

    @Override
    public ClusterState clusterState() {
        ClusterState clusterState = committedState.get();
        assert clusterState != null : "accessing cluster state before it is set";
        return clusterState;
    }

    @Override
    public void publish(ClusterChangedEvent clusterChangedEvent, AckListener ackListener) {
        ClusterState newState = clusterChangedEvent.state();
        assert newState.getNodes().isLocalNodeElectedMaster() : "Shouldn't publish state when not master " + clusterChangedEvent.source();

        // state got changed locally (maybe because another master published to us)
        if (clusterChangedEvent.previousState() != this.committedState.get()) {
            throw new FailedToCommitClusterStateException("state was mutated while calculating new CS update");
        }

        pendingStatesQueue.addPending(newState);

        try {
            publishClusterState.publish(clusterChangedEvent, electMaster.minimumMasterNodes(), ackListener);
        } catch (FailedToCommitClusterStateException t) {
            // cluster service logs a WARN message
            logger.debug("failed to publish cluster state version [{}] (not enough nodes acknowledged, min master nodes [{}])",
                newState.version(), electMaster.minimumMasterNodes());

            synchronized (stateMutex) {
                pendingStatesQueue.failAllStatesAndClear(
                    new ElasticsearchException("failed to publish cluster state"));

                rejoin("zen-disco-failed-to-publish");
            }
            throw t;
        }

        final DiscoveryNode localNode = newState.getNodes().getLocalNode();
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicBoolean processedOrFailed = new AtomicBoolean();
        pendingStatesQueue.markAsCommitted(newState.stateUUID(),
            new PendingClusterStatesQueue.StateProcessedListener() {
                @Override
                public void onNewClusterStateProcessed() {
                    processedOrFailed.set(true);
                    latch.countDown();
                    ackListener.onNodeAck(localNode, null);
                }

                @Override
                public void onNewClusterStateFailed(Exception e) {
                    processedOrFailed.set(true);
                    latch.countDown();
                    ackListener.onNodeAck(localNode, e);
                    logger.warn(() -> new ParameterizedMessage(
                            "failed while applying cluster state locally [{}]", clusterChangedEvent.source()), e);
                }
            });

        synchronized (stateMutex) {
            if (clusterChangedEvent.previousState() != this.committedState.get()) {
                throw new FailedToCommitClusterStateException("local state was mutated while CS update was published to other nodes");
            }

            boolean sentToApplier = processNextCommittedClusterState("master " + newState.nodes().getMasterNode() +
                " committed version [" + newState.version() + "] source [" + clusterChangedEvent.source() + "]");
            if (sentToApplier == false && processedOrFailed.get() == false) {
                assert false : "cluster state published locally neither processed nor failed: " + newState;
                logger.warn("cluster state with version [{}] that is published locally has neither been processed nor failed",
                    newState.version());
                return;
            }
        }
        // indefinitely wait for cluster state to be applied locally
        try {
            latch.await(); // NOTE:htt, 等待集群状态完毕
        } catch (InterruptedException e) {
            logger.debug(() -> new ParameterizedMessage(
                    "interrupted while applying cluster state locally [{}]", clusterChangedEvent.source()), e);
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Gets the current set of nodes involved in the node fault detection.
     * NB: for testing purposes
     */
    Set<DiscoveryNode> getFaultDetectionNodes() {
        return nodesFD.getNodes();
    }

    @Override
    public DiscoveryStats stats() {
        return new DiscoveryStats(pendingStatesQueue.stats(), publishClusterState.stats());
    }

    public DiscoverySettings getDiscoverySettings() {
        return discoverySettings;
    }

    /**
     * returns true if zen discovery is started and there is a currently a background thread active for (re)joining
     * the cluster used for testing.
     */
    public boolean joiningCluster() {
        return joinThreadControl.joinThreadActive();
    }

    // used for testing
    public ClusterState[] pendingClusterStates() {
        return pendingStatesQueue.pendingClusterStates();
    }

    PendingClusterStatesQueue pendingClusterStatesQueue() {
        return pendingStatesQueue;
    }

    /**
     * the main function of a join thread. This function is guaranteed to join the cluster
     * or spawn a new join thread upon failure to do so.
     */
    // NOTE:htt, 内部join集群的主要函数。此函数保证加入集群或在新线程上失败时生成新线程。
    private void innerJoinCluster() { // NOTE:htt, 内部join集群的主要函数。此函数保证加入集群或在新线程上失败时生成新线程。
        DiscoveryNode masterNode = null; // NOTE:htt, 当前master节点
        final Thread currentThread = Thread.currentThread(); // NOTE:htt, 当前线程
        nodeJoinController.startElectionContext(); // NOTE:htt, 启动选举上下文
        while (masterNode == null && joinThreadControl.joinThreadActive(currentThread)) { // NOTE:htt, 如果当前master节点为null且当前线程是活跃的join线程，则继续循环
            masterNode = findMaster(); // NOTE:htt, 查找master节点
        }

        if (!joinThreadControl.joinThreadActive(currentThread)) { // NOTE:htt, 如果当前线程不是活跃的join线程，则返回
            logger.trace("thread is no longer in currentJoinThread. Stopping."); // NOTE:htt, 打印日志
            return; // NOTE:htt, 返回
        }

        if (transportService.getLocalNode().equals(masterNode)) { // NOTE:htt, 如果当前节点是master节点，则等待其他节点加入
            final int requiredJoins = Math.max(0, electMaster.minimumMasterNodes() - 1); // we count as one // NOTE: htt, 选主时最少多少个节点需要确认，要满足 majority 机制
            logger.debug("elected as master, waiting for incoming joins ([{}] needed)", requiredJoins); // NOTE:htt, 打印日志
            nodeJoinController.waitToBeElectedAsMaster(requiredJoins, masterElectionWaitForJoinsTimeout, // NOTE:htt, 等待成为master
                    new NodeJoinController.ElectionCallback() { // NOTE:htt, 选举回调
                        @Override
                        public void onElectedAsMaster(ClusterState state) { // NOTE:htt, 当选为master
                            synchronized (stateMutex) { // NOTE:htt, 同步状态互斥锁
                                joinThreadControl.markThreadAsDone(currentThread); // NOTE:htt, 标记当前线程为完成
                            }
                        }

                        @Override
                        public void onFailure(Throwable t) { // NOTE:htt, 选举失败
                            logger.trace("failed while waiting for nodes to join, rejoining", t); // NOTE:htt, 打印日志
                            synchronized (stateMutex) { // NOTE:htt, 同步状态互斥锁
                                joinThreadControl.markThreadAsDoneAndStartNew(currentThread); // NOTE:htt, 标记当前线程为完成并启动新线程
                            }
                        }
                    }

            );
        } else {
            // process any incoming joins (they will fail because we are not the master)
            // NOTE:htt, 处理任何传入的join请求（因为我们是master，所以它们会失败）
            nodeJoinController.stopElectionContext(masterNode + " elected"); // NOTE:htt, 停止选举上下文

            // send join request
            // NOTE:htt, 发送join请求   
            final boolean success = joinElectedMaster(masterNode); // NOTE:htt, 发送join请求

            synchronized (stateMutex) { // NOTE:htt, 同步状态互斥锁
                if (success) { // NOTE:htt, 如果发送join请求成功
                    DiscoveryNode currentMasterNode = this.clusterState().getNodes().getMasterNode(); // NOTE:htt, 获取当前master节点
                    if (currentMasterNode == null) { // NOTE:htt, 如果当前master节点为null
                        // Post 1.3.0, the master should publish a new cluster state before acking our join request. we now should have
                        // a valid master.
                        // NOTE:htt, 1.3.0版本后，master应该在ack我们的join请求之前发布新的集群状态。我们现在应该有一个有效的master。
                        logger.debug("no master node is set, despite of join request completing. retrying pings."); // NOTE:htt, 打印日志
                        joinThreadControl.markThreadAsDoneAndStartNew(currentThread); // NOTE:htt, 标记当前线程为完成并启动新线程
                    } else if (currentMasterNode.equals(masterNode) == false) { // NOTE:htt, 如果当前master节点不等于master节点
                        // update cluster state
                        joinThreadControl.stopRunningThreadAndRejoin("master_switched_while_finalizing_join"); // NOTE:htt, 停止当前线程并重新加入集群
                    }

                    joinThreadControl.markThreadAsDone(currentThread); // NOTE:htt, 标记当前线程为完成
                } else {
                    // failed to join. Try again...
                    joinThreadControl.markThreadAsDoneAndStartNew(currentThread); // NOTE:htt, 标记当前线程为完成并启动新线程
                }
            }
        }
    }

    /**
     * Join a newly elected master.
     *
     * @return true if successful
     */
    private boolean joinElectedMaster(DiscoveryNode masterNode) {
        try {
            // first, make sure we can connect to the master
            transportService.connectToNode(masterNode);
        } catch (Exception e) {
            logger.warn(() -> new ParameterizedMessage("failed to connect to master [{}], retrying...", masterNode), e);
            return false;
        }
        int joinAttempt = 0; // we retry on illegal state if the master is not yet ready
        while (true) {
            try {
                logger.trace("joining master {}", masterNode);
                membership.sendJoinRequestBlocking(masterNode, transportService.getLocalNode(), joinTimeout);
                return true;
            } catch (Exception e) {
                final Throwable unwrap = ExceptionsHelper.unwrapCause(e);
                if (unwrap instanceof NotMasterException) { // NOTE: htt, 如果遇到 NotMasterExeception，则重试3次，等待Master当选为master
                    if (++joinAttempt == this.joinRetryAttempts) { // NOTE: htt, try to join master 3 times
                        logger.info("failed to send join request to master [{}], reason [{}], tried [{}] times", masterNode, ExceptionsHelper.detailedMessage(e), joinAttempt);
                        return false;
                    } else {
                        logger.trace("master {} failed with [{}]. retrying... (attempts done: [{}])", masterNode, ExceptionsHelper.detailedMessage(e), joinAttempt);
                    }
                } else { // NOTE: htt, 如果是超时等异常，当前是1min超时，则直接继续下一次选举
                    if (logger.isTraceEnabled()) {
                        logger.trace(() -> new ParameterizedMessage("failed to send join request to master [{}]", masterNode), e);
                    } else {
                        logger.info("failed to send join request to master [{}], reason [{}]", masterNode, ExceptionsHelper.detailedMessage(e));
                    }
                    return false;
                }
            }

            try {
                Thread.sleep(this.joinRetryDelay.millis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void submitRejoin(String source) {
        synchronized (stateMutex) {
            rejoin(source);
        }
    }

    // visible for testing
    void setCommittedState(ClusterState clusterState) {
        synchronized (stateMutex) {
            committedState.set(clusterState);
        }
    }

    // visible for testing
    static class NodeRemovalClusterStateTaskExecutor implements ClusterStateTaskExecutor<NodeRemovalClusterStateTaskExecutor.Task>, ClusterStateTaskListener {

        private final AllocationService allocationService;
        private final ElectMasterService electMasterService;
        private final Consumer<String> rejoin;
        private final Logger logger;

        static class Task {

            private final DiscoveryNode node;
            private final String reason;

            Task(final DiscoveryNode node, final String reason) {
                this.node = node;
                this.reason = reason;
            }

            public DiscoveryNode node() {
                return node;
            }

            public String reason() {
                return reason;
            }

            @Override
            public String toString() {
                return node + " " + reason;
            }
        }

        NodeRemovalClusterStateTaskExecutor(
                final AllocationService allocationService,
                final ElectMasterService electMasterService,
                final Consumer<String> rejoin,
                final Logger logger) {
            this.allocationService = allocationService;
            this.electMasterService = electMasterService;
            this.rejoin = rejoin;
            this.logger = logger;
        }

        @Override
        public ClusterTasksResult<Task> execute(final ClusterState currentState, final List<Task> tasks) throws Exception {
            final DiscoveryNodes.Builder remainingNodesBuilder = DiscoveryNodes.builder(currentState.nodes());
            boolean removed = false;
            for (final Task task : tasks) {
                if (currentState.nodes().nodeExists(task.node())) {
                    remainingNodesBuilder.remove(task.node());
                    removed = true;
                } else {
                    logger.debug("node [{}] does not exist in cluster state, ignoring", task);
                }
            }

            if (!removed) {
                // no nodes to remove, keep the current cluster state
                return ClusterTasksResult.<Task>builder().successes(tasks).build(currentState);
            }

            final ClusterState remainingNodesClusterState = remainingNodesClusterState(currentState, remainingNodesBuilder);

            final ClusterTasksResult.Builder<Task> resultBuilder = ClusterTasksResult.<Task>builder().successes(tasks);
            if (electMasterService.hasEnoughMasterNodes(remainingNodesClusterState.nodes()) == false) {
                final int masterNodes = electMasterService.countMasterNodes(remainingNodesClusterState.nodes());
                rejoin.accept(LoggerMessageFormat.format("not enough master nodes (has [{}], but needed [{}])",
                                                         masterNodes, electMasterService.minimumMasterNodes()));
                return resultBuilder.build(currentState);
            } else {
                return resultBuilder.build(allocationService.deassociateDeadNodes(remainingNodesClusterState, true, describeTasks(tasks)));
            }
        }

        // visible for testing
        // hook is used in testing to ensure that correct cluster state is used to test whether a
        // rejoin or reroute is needed
        ClusterState remainingNodesClusterState(final ClusterState currentState, DiscoveryNodes.Builder remainingNodesBuilder) {
            return ClusterState.builder(currentState).nodes(remainingNodesBuilder).build();
        }

        @Override
        public void onFailure(final String source, final Exception e) {
            logger.error(() -> new ParameterizedMessage("unexpected failure during [{}]", source), e);
        }

        @Override
        public void onNoLongerMaster(String source) {
            logger.debug("no longer master while processing node removal [{}]", source);
        }

    }

    private void removeNode(final DiscoveryNode node, final String source, final String reason) {
        masterService.submitStateUpdateTask(
                source + "(" + node + "), reason(" + reason + ")",
                new NodeRemovalClusterStateTaskExecutor.Task(node, reason),
                ClusterStateTaskConfig.build(Priority.IMMEDIATE),
                nodeRemovalExecutor,
                nodeRemovalExecutor);
    }

    private void handleLeaveRequest(final DiscoveryNode node) {
        if (lifecycleState() != Lifecycle.State.STARTED) {
            // not started, ignore a node failure
            return;
        }
        if (localNodeMaster()) {
            removeNode(node, "zen-disco-node-left", "left");
        } else if (node.equals(clusterState().nodes().getMasterNode())) {
            handleMasterGone(node, null, "shut_down");
        }
    }

    private void handleNodeFailure(final DiscoveryNode node, final String reason) {
        if (lifecycleState() != Lifecycle.State.STARTED) {
            // not started, ignore a node failure
            return;
        }
        if (!localNodeMaster()) {
            // nothing to do here...
            return;
        }
        removeNode(node, "zen-disco-node-failed", reason);
    }

    private void handleMinimumMasterNodesChanged(final int minimumMasterNodes) {
        if (lifecycleState() != Lifecycle.State.STARTED) {
            // not started, ignore a node failure
            return;
        }
        final int prevMinimumMasterNode = ZenDiscovery.this.electMaster.minimumMasterNodes();
        ZenDiscovery.this.electMaster.minimumMasterNodes(minimumMasterNodes);
        if (!localNodeMaster()) {
            // We only set the new value. If the master doesn't see enough nodes it will revoke it's mastership.
            return;
        }
        synchronized (stateMutex) {
            // check if we have enough master nodes, if not, we need to move into joining the cluster again
            if (!electMaster.hasEnoughMasterNodes(committedState.get().nodes())) {
                rejoin("not enough master nodes on change of minimum_master_nodes from [" + prevMinimumMasterNode + "] to [" + minimumMasterNodes + "]");
            }
        }
    }

    private void handleMasterGone(final DiscoveryNode masterNode, final Throwable cause, final String reason) {
        if (lifecycleState() != Lifecycle.State.STARTED) {
            // not started, ignore a master failure
            return;
        }
        if (localNodeMaster()) {
            // we might get this on both a master telling us shutting down, and then the disconnect failure
            return;
        }

        logger.info(() -> new ParameterizedMessage("master_left [{}], reason [{}]", masterNode, reason), cause);

        synchronized (stateMutex) {
            if (localNodeMaster() == false && masterNode.equals(committedState.get().nodes().getMasterNode())) {
                // flush any pending cluster states from old master, so it will not be set as master again
                pendingStatesQueue.failAllStatesAndClear(new ElasticsearchException("master left [{}]", reason));
                rejoin("master left (reason = " + reason + ")");
            }
        }
    }

    // return true if state has been sent to applier
    boolean processNextCommittedClusterState(String reason) {
        assert Thread.holdsLock(stateMutex);

        final ClusterState newClusterState = pendingStatesQueue.getNextClusterStateToProcess(); // NOTE:htt, 获取下一个待处理的集群状态
        final ClusterState currentState = committedState.get();
        // all pending states have been processed
        if (newClusterState == null) {
            return false;
        }

        assert newClusterState.nodes().getMasterNode() != null : "received a cluster state without a master";
        assert !newClusterState.blocks().hasGlobalBlock(discoverySettings.getNoMasterBlock()) : "received a cluster state with a master block";

        if (currentState.nodes().isLocalNodeElectedMaster() && newClusterState.nodes().isLocalNodeElectedMaster() == false) {
            handleAnotherMaster(currentState, newClusterState.nodes().getMasterNode(), newClusterState.version(), "via a new cluster state");
            return false;
        }

        try {
            if (shouldIgnoreOrRejectNewClusterState(logger, currentState, newClusterState)) {
                String message = String.format(
                    Locale.ROOT,
                    "rejecting cluster state version [%d] uuid [%s] received from [%s]",
                    newClusterState.version(),
                    newClusterState.stateUUID(),
                    newClusterState.nodes().getMasterNodeId()
                ); // NOTE:htt, 是否拒绝新集群状态（如果master不一致，获取新集群状态更旧，则拒绝新的集群状态）
                throw new IllegalStateException(message);
            }
        } catch (Exception e) {
            try {
                pendingStatesQueue.markAsFailed(newClusterState, e);
            } catch (Exception inner) {
                inner.addSuppressed(e);
                logger.error(() -> new ParameterizedMessage("unexpected exception while failing [{}]", reason), inner);
            }
            return false;
        }

        if (currentState.blocks().hasGlobalBlock(discoverySettings.getNoMasterBlock())) {
            // its a fresh update from the master as we transition from a start of not having a master to having one
            logger.debug("got first state from fresh master [{}]", newClusterState.nodes().getMasterNodeId());
        }

        if (currentState == newClusterState) {
            return false;
        }

        committedState.set(newClusterState); // NOTE:htt, 实则最后的更新的state

        // update failure detection only after the state has been updated to prevent race condition with handleLeaveRequest
        // and handleNodeFailure as those check the current state to determine whether the failure is to be handled by this node
        if (newClusterState.nodes().isLocalNodeElectedMaster()) {
            // update the set of nodes to ping
            nodesFD.updateNodesAndPing(newClusterState); // NOTE: htt, 新集群状态下，如果探测节点不在集群列表中则移除；如果集群列表中节点不在探测列表中则加入探测
        } else {
            // check to see that we monitor the correct master of the cluster
            if (masterFD.masterNode() == null || !masterFD.masterNode().equals(newClusterState.nodes().getMasterNode())) {
                masterFD.restart(newClusterState.nodes().getMasterNode(),
                    "new cluster state received and we are monitoring the wrong master [" + masterFD.masterNode() + "]");
            }
        }

        clusterApplier.onNewClusterState("apply cluster state (from master [" + reason + "])",
            this::clusterState,
            new ClusterStateTaskListener() {
                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    try {
                        pendingStatesQueue.markAsProcessed(newClusterState);
                    } catch (Exception e) {
                        onFailure(source, e);
                    }
                }

                @Override
                public void onFailure(String source, Exception e) {
                    logger.error(() -> new ParameterizedMessage("unexpected failure applying [{}]", reason), e);
                    try {
                        // TODO: use cluster state uuid instead of full cluster state so that we don't keep reference to CS around
                        // for too long.
                        pendingStatesQueue.markAsFailed(newClusterState, e);
                    } catch (Exception inner) {
                        inner.addSuppressed(e);
                        logger.error(() -> new ParameterizedMessage("unexpected exception while failing [{}]", reason), inner);
                    }
                }
            }); // NOTE:htt, 应用新集群状态

        return true;
    }

    /**
     * In the case we follow an elected master the new cluster state needs to have the same elected master and
     * the new cluster state version needs to be equal or higher than our cluster state version.
     * If the first condition fails we reject the cluster state and throw an error.
     * If the second condition fails we ignore the cluster state.
     */
    public static boolean shouldIgnoreOrRejectNewClusterState(Logger logger, ClusterState currentState, ClusterState newClusterState) { // NOTE:htt, 是否拒绝新集群状态（如果master不一致，获取新集群状态更旧，则拒绝新的集群状态）
        validateStateIsFromCurrentMaster(logger, currentState.nodes(), newClusterState); // NOTE:htt, 集群状态更新的master的节点必须一致

        // reject cluster states that are not new from the same master
        if (currentState.supersedes(newClusterState) || // NOTE:htt, 当前集群状态version更新，则忽略新的集群状态
                (newClusterState.nodes().getMasterNodeId().equals(currentState.nodes().getMasterNodeId()) && currentState.version() == newClusterState.version())) { // NOTE:htt, 当前集群状态version和新集群状态version一致则忽略新集群状态
            // if the new state has a smaller version, and it has the same master node, then no need to process it
            logger.debug("received a cluster state that is not newer than the current one, ignoring (received {}, current {})", newClusterState.version(), currentState.version());
            return true;
        }

        // reject older cluster states if we are following a master
        if (currentState.nodes().getMasterNodeId() != null && newClusterState.version() < currentState.version()) { // NOTE:htt, 如果新集群状态version更小，则忽略新集群状态
            logger.debug("received a cluster state that has a lower version than the current one, ignoring (received {}, current {})", newClusterState.version(), currentState.version());
            return true;
        }
        return false;
    }

    /**
     * In the case we follow an elected master the new cluster state needs to have the same elected master
     * This method checks for this and throws an exception if needed
     */

    public static void validateStateIsFromCurrentMaster(Logger logger, DiscoveryNodes currentNodes, ClusterState newClusterState) { // NOTE:htt, 集群状态更新的master的节点必须一致
        if (currentNodes.getMasterNodeId() == null) { // NOTE:htt, 当前无master，则允许使用新集群状态
            return;
        }
        if (!currentNodes.getMasterNodeId().equals(newClusterState.nodes().getMasterNodeId())) { // NOTE:htt, 如果当前集群状态的master节点和新集群状态master节点不一致，则出错
            logger.warn("received a cluster state from a different master than the current one, rejecting (received {}, current {})", newClusterState.nodes().getMasterNode(), currentNodes.getMasterNode());
            throw new IllegalStateException("cluster state from a different master than the current one, rejecting (received " + newClusterState.nodes().getMasterNode() + ", current " + currentNodes.getMasterNode() + ")");
        }
    }

    void handleJoinRequest(final DiscoveryNode node, final ClusterState state, final MembershipAction.JoinCallback callback) {
        if (nodeJoinController == null) {
            throw new IllegalStateException("discovery module is not yet started");
        } else {
            // we do this in a couple of places including the cluster update thread. This one here is really just best effort
            // to ensure we fail as fast as possible.
            onJoinValidators.stream().forEach(a -> a.accept(node, state));
            if (state.getBlocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK) == false) {
                MembershipAction.ensureMajorVersionBarrier(node.getVersion(), state.getNodes().getMinNodeVersion());
            }
            // try and connect to the node, if it fails, we can raise an exception back to the client...
            transportService.connectToNode(node);

            // validate the join request, will throw a failure if it fails, which will get back to the
            // node calling the join request
            try {
                membership.sendValidateJoinRequestBlocking(node, state, joinTimeout);
            } catch (Exception e) {
                logger.warn(() -> new ParameterizedMessage("failed to validate incoming join request from node [{}]", node),
                    e);
                callback.onFailure(new IllegalStateException("failure when sending a validation request to node", e));
                return;
            }
            nodeJoinController.handleJoinRequest(node, callback);
        }
    }

    private DiscoveryNode findMaster() { // NOTE:htt, 获取master节点
        logger.trace("starting to ping");
        List<ZenPing.PingResponse> fullPingResponses = pingAndWait(pingTimeout).toList(); // NOTE:htt, ping节点，并且获取对应节点结果，同时支持超时机制
        if (fullPingResponses == null) { // NOTE:htt, 如果没有找到，则返回null
            logger.trace("No full ping responses");
            return null;
        }
        if (logger.isTraceEnabled()) { // NOTE:htt, 如果是trace级别日志，则打印回包列表
            StringBuilder sb = new StringBuilder();
            if (fullPingResponses.size() == 0) {
                sb.append(" {none}");
            } else {
                for (ZenPing.PingResponse pingResponse : fullPingResponses) {
                    sb.append("\n\t--> ").append(pingResponse);
                }
            }
            logger.trace("full ping responses:{}", sb);
        }

        final DiscoveryNode localNode = transportService.getLocalNode(); // NOTE:htt, 获取本地节点

        // add our selves
        assert fullPingResponses.stream().map(ZenPing.PingResponse::node)
            .filter(n -> n.equals(localNode)).findAny().isPresent() == false; // NOTE:htt, 不包含自身节点

        fullPingResponses.add(new ZenPing.PingResponse(localNode, null, this.clusterState())); // NOTE:htt, 添加自身节点作为回包

        // filter responses
        final List<ZenPing.PingResponse> pingResponses = filterPingResponses(fullPingResponses, masterElectionIgnoreNonMasters, logger); // NOTE:htt, 根据masterElectionIgnoreNonMasters处理回包列表

        List<DiscoveryNode> activeMasters = new ArrayList<>(); // NOTE:htt, 选择激活的master节点
        for (ZenPing.PingResponse pingResponse : pingResponses) {
            // We can't include the local node in pingMasters list, otherwise we may up electing ourselves without
            // any check / verifications from other nodes in ZenDiscover#innerJoinCluster()
            if (pingResponse.master() != null && !localNode.equals(pingResponse.master())) { // NOTE:htt, 如果回包中master节点不为空，并且部署本地节点则添加到激活master节点中
                activeMasters.add(pingResponse.master()); // NOTE: htt, add current active master which not including current local node
            }
        }

        // nodes discovered during pinging
        List<ElectMasterService.MasterCandidate> masterCandidates = new ArrayList<>(); // NOTE:htt, 添加master节点
        for (ZenPing.PingResponse pingResponse : pingResponses) {
            if (pingResponse.node().isMasterNode()) { // NOTE: htt, add master candidate note，这里仅包含角色是 Master的节点
                masterCandidates.add(new ElectMasterService.MasterCandidate(pingResponse.node(), pingResponse.getClusterStateVersion())); // NOTE:htt, 添加master节点
            }
        }

        if (activeMasters.isEmpty()) { // NOTE:htt, 如果激活master节点为空，则从候选master节点选择一个
            if (electMaster.hasEnoughCandidates(masterCandidates)) { // NOTE: htt, 判断是否有足够的候选节点, number of candidate nodes >= minimum master nodes
                final ElectMasterService.MasterCandidate winner = electMaster.electMaster(masterCandidates); // NOTE: htt, 候选节点个数满足要求，然后进行排序(先判断版本号(大的在前），若相等判断节点信息，id小的为在前)，再返回排序第一个候选节点作为master节点
                logger.trace("candidate {} won election", winner);
                return winner.getNode(); // NOTE:htt, 返回成功赢得选举节点
            } else { // NOTE:htt, 不够半数则返回null
                // if we don't have enough master nodes, we bail, because there are not enough master to elect from
                logger.warn("not enough master nodes discovered during pinging (found [{}], but needed [{}]), pinging again",
                            masterCandidates, electMaster.minimumMasterNodes());
                return null;
            }
        } else {
            assert !activeMasters.contains(localNode) : "local node should never be elected as master when other nodes indicate an active master";
            // lets tie break between discovered nodes
            return electMaster.tieBreakActiveMasters(activeMasters); // NOTE:htt, 从激活的master节点中选择id最小的作为master节点
        }
    }

    static List<ZenPing.PingResponse> filterPingResponses(List<ZenPing.PingResponse> fullPingResponses, boolean masterElectionIgnoreNonMasters, Logger logger) { // NOTE:htt, 根据masterElectionIgnoreNonMasters处理回包列表
        List<ZenPing.PingResponse> pingResponses;
        if (masterElectionIgnoreNonMasters) { // NOTE: htt, 如果 忽略 非master节点，则只保留 master的response
            pingResponses = fullPingResponses.stream().filter(ping -> ping.node().isMasterNode()).collect(Collectors.toList());
        } else {
            pingResponses = fullPingResponses; // NOTE: htt, 如果 不忽略非master节点，则留下全部 response
        }

        if (logger.isDebugEnabled()) { // NOTE:htt, 如果debug级别，打印回包日志
            StringBuilder sb = new StringBuilder();
            if (pingResponses.isEmpty()) {
                sb.append(" {none}");
            } else {
                for (ZenPing.PingResponse pingResponse : pingResponses) {
                    sb.append("\n\t--> ").append(pingResponse);
                }
            }
            logger.debug("filtered ping responses: (ignore_non_masters [{}]){}", masterElectionIgnoreNonMasters, sb);
        }
        return pingResponses; // NOTE:htt, 返回处理的ping回包
    }

    protected void rejoin(String reason) { // NOTE:htt, 重新加入集群
        assert Thread.holdsLock(stateMutex); // NOTE:htt, 确保当前线程持有stateMutex锁
        ClusterState clusterState = committedState.get(); // NOTE:htt, 获取最后提交的集群状态

        logger.warn("{}, current nodes: {}", reason, clusterState.nodes()); // NOTE:htt, 打印当前集群状态
        nodesFD.stop(); // NOTE:htt, 停止对所有其他节点探测（当前角色是主master节点）
        masterFD.stop(reason); // NOTE:htt, 停止对master节点探测（当前角色是非主master 节点，如数据节点）

        // TODO: do we want to force a new thread if we actively removed the master? this is to give a full pinging cycle
        // before a decision is made.
        // NOTE:htt, TODO: 是否需要强制新线程，如果主动移除了主节点？
        // 如果主动移除了主节点，则需要强制新线程，以确保在做出决策之前进行完整的pinging周期。
        joinThreadControl.startNewThreadIfNotRunning(); // NOTE:htt, 重新发起探测

        if (clusterState.nodes().getMasterNodeId() != null) { // NOTE:htt, 如果当前集群状态有主节点，则移除主节点
            // remove block if it already exists before adding new one
            assert clusterState.blocks().hasGlobalBlock(discoverySettings.getNoMasterBlock().id()) == false : // NOTE:htt, 确保当前集群状态没有NO_MASTER_BLOCK
                "NO_MASTER_BLOCK should only be added by ZenDiscovery"; // NOTE:htt, NO_MASTER_BLOCK应该仅由ZenDiscovery添加
            ClusterBlocks clusterBlocks = ClusterBlocks.builder().blocks(clusterState.blocks()) // NOTE:htt, 创建新的集群状态
                .addGlobalBlock(discoverySettings.getNoMasterBlock()) // NOTE:htt, 添加NO_MASTER_BLOCK
                .build(); // NOTE:htt, 构建新的集群状态

            DiscoveryNodes discoveryNodes = new DiscoveryNodes.Builder(clusterState.nodes()).masterNodeId(null).build(); // NOTE:htt, 创建新的集群状态
            clusterState = ClusterState.builder(clusterState) // NOTE:htt, 创建新的集群状态
                .blocks(clusterBlocks) // NOTE:htt, 添加NO_MASTER_BLOCK
                .nodes(discoveryNodes) // NOTE:htt, 移除主节点，因discoveryNodes中master节点id为null
                .build(); // NOTE:htt, 构建新的集群状态

            committedState.set(clusterState); // NOTE:htt, 设置新的集群状态
            clusterApplier.onNewClusterState(reason, this::clusterState, (source, e) -> {}); // NOTE:htt, 应用新的集群状态，不等待集群状态应用完成
        }
    }

    private boolean localNodeMaster() {
        return clusterState().nodes().isLocalNodeElectedMaster();
    }

    private void handleAnotherMaster(ClusterState localClusterState, final DiscoveryNode otherMaster, long otherClusterStateVersion, String reason) {
        assert localClusterState.nodes().isLocalNodeElectedMaster() : "handleAnotherMaster called but current node is not a master";
        assert Thread.holdsLock(stateMutex);

        if (otherClusterStateVersion > localClusterState.version()) {
            rejoin("zen-disco-discovered another master with a new cluster_state [" + otherMaster + "][" + reason + "]");
        } else {
            // TODO: do this outside mutex
            logger.warn("discovered [{}] which is also master but with an older cluster_state, telling [{}] to rejoin the cluster ([{}])", otherMaster, otherMaster, reason);
            try {
                // make sure we're connected to this node (connect to node does nothing if we're already connected)
                // since the network connections are asymmetric, it may be that we received a state but have disconnected from the node
                // in the past (after a master failure, for example)
                transportService.connectToNode(otherMaster);
                transportService.sendRequest(otherMaster, DISCOVERY_REJOIN_ACTION_NAME, new RejoinClusterRequest(localClusterState.nodes().getLocalNodeId()), new EmptyTransportResponseHandler(ThreadPool.Names.SAME) {

                    @Override
                    public void handleException(TransportException exp) {
                        logger.warn(() -> new ParameterizedMessage("failed to send rejoin request to [{}]", otherMaster), exp);
                    }
                });
            } catch (Exception e) {
                logger.warn(() -> new ParameterizedMessage("failed to send rejoin request to [{}]", otherMaster), e);
            }
        }
    }

    private ZenPing.PingCollection pingAndWait(TimeValue timeout) { // NOTE:htt, ping节点，并且获取对应节点结果，同时支持超时机制
        final CompletableFuture<ZenPing.PingCollection> response = new CompletableFuture<>();
        try {
            zenPing.ping(response::complete, timeout); // NOTE:htt, ping节点，并支持超时机制
        } catch (Exception ex) {
            // logged later
            response.completeExceptionally(ex);
        }

        try {
            return response.get(); // NOTE:htt, 返回ping各个节点结果列表
        } catch (InterruptedException e) {
            logger.trace("pingAndWait interrupted");
            return new ZenPing.PingCollection(); // NOTE:htt, 中断异常返回空的ping结果
        } catch (ExecutionException e) {
            logger.warn("Ping execution failed", e);
            return new ZenPing.PingCollection(); // NOTE:htt, ping异常返回空的ping列表
        }
    }

    @Override
    public void onIncomingClusterState(ClusterState incomingState) { // NOTE:htt, 添加新的集群状态到队列中，待收到master的commit请求后再执行集群状态更新
        validateIncomingState(logger, incomingState, committedState.get()); // NOTE:htt, 验证新的集群状态必须可用，才继续下一步
        pendingStatesQueue.addPending(incomingState); // NOTE:htt, 添加新的集群状态到队列中，待收到master的commit请求后再执行集群状态
    }

    @Override
    public void onClusterStateCommitted(String stateUUID, ActionListener<Void> processedListener) {
        final ClusterState state = pendingStatesQueue.markAsCommitted(stateUUID,
            new PendingClusterStatesQueue.StateProcessedListener() {
                @Override
                public void onNewClusterStateProcessed() {
                    processedListener.onResponse(null); // NOTE:htt, 集群状态正常更新，即commit完成，则回包给master
                }

                @Override
                public void onNewClusterStateFailed(Exception e) {
                    processedListener.onFailure(e); // NOTE:htt, 集群状态更新异常，即commit失败，回包给master
                }
            });
        if (state != null) {
            synchronized (stateMutex) {
                processNextCommittedClusterState("master " + state.nodes().getMasterNode() +
                    " committed version [" + state.version() + "]");
            }
        }
    }

    /**
     * does simple sanity check of the incoming cluster state. Throws an exception on rejections.
     */
    static void validateIncomingState(Logger logger, ClusterState incomingState, ClusterState lastState) { // NOTE:htt, 验证新的集群状态是否可以采用
        final ClusterName incomingClusterName = incomingState.getClusterName();
        if (!incomingClusterName.equals(lastState.getClusterName())) { // NOTE:htt, 集群名必须一致
            logger.warn("received cluster state from [{}] which is also master but with a different cluster name [{}]",
                incomingState.nodes().getMasterNode(), incomingClusterName);
            throw new IllegalStateException("received state from a node that is not part of the cluster");
        }
        if (lastState.nodes().getLocalNode().equals(incomingState.nodes().getLocalNode()) == false) { // NOTE:htt, 当前节点必须一致
            logger.warn("received a cluster state from [{}] and not part of the cluster, should not happen",
                incomingState.nodes().getMasterNode());
            throw new IllegalStateException("received state with a local node that does not match the current local node");
        }

        if (shouldIgnoreOrRejectNewClusterState(logger, lastState, incomingState)) { // NOTE:htt, 是否拒绝新集群状态（如果master不一致，获取新集群状态更旧，则拒绝新的集群状态）
            String message = String.format(
                Locale.ROOT,
                "rejecting cluster state version [%d] uuid [%s] received from [%s]",
                incomingState.version(),
                incomingState.stateUUID(),
                incomingState.nodes().getMasterNodeId()
            );
            logger.warn(message);
            throw new IllegalStateException(message); // NOTE:htt, 拒绝集群状态，则抛出异常
        }

    }

    private class MembershipListener implements MembershipAction.MembershipListener {
        @Override
        public void onJoin(DiscoveryNode node, MembershipAction.JoinCallback callback) {
            handleJoinRequest(node, ZenDiscovery.this.clusterState(), callback);
        }

        @Override
        public void onLeave(DiscoveryNode node) {
            handleLeaveRequest(node);
        }
    }

    private class NodeFaultDetectionListener extends NodesFaultDetection.Listener {

        private final AtomicInteger pingsWhileMaster = new AtomicInteger(0);

        @Override
        public void onNodeFailure(DiscoveryNode node, String reason) {
            handleNodeFailure(node, reason);
        }

        @Override
        public void onPingReceived(final NodesFaultDetection.PingRequest pingRequest) {
            // if we are master, we don't expect any fault detection from another node. If we get it
            // means we potentially have two masters in the cluster.
            if (!localNodeMaster()) {
                pingsWhileMaster.set(0);
                return;
            }

            if (pingsWhileMaster.incrementAndGet() < maxPingsFromAnotherMaster) {
                logger.trace("got a ping from another master {}. current ping count: [{}]", pingRequest.masterNode(), pingsWhileMaster.get());
                return;
            }
            logger.debug("got a ping from another master {}. resolving who should rejoin. current ping count: [{}]", pingRequest.masterNode(), pingsWhileMaster.get());
            synchronized (stateMutex) {
                ClusterState currentState = committedState.get();
                if (currentState.nodes().isLocalNodeElectedMaster()) {
                    pingsWhileMaster.set(0);
                    handleAnotherMaster(currentState, pingRequest.masterNode(), pingRequest.clusterStateVersion(), "node fd ping");
                }
            }
        }
    }

    private class MasterNodeFailureListener implements MasterFaultDetection.Listener {

        @Override
        public void onMasterFailure(DiscoveryNode masterNode, Throwable cause, String reason) {
            handleMasterGone(masterNode, cause, reason);
        }
    }

    public static class RejoinClusterRequest extends TransportRequest { // NOTE:htt, 重新加入集群请求

        private String fromNodeId; // NOTE:htt, 加入节点

        RejoinClusterRequest(String fromNodeId) {
            this.fromNodeId = fromNodeId;
        }

        public RejoinClusterRequest() {
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            fromNodeId = in.readOptionalString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalString(fromNodeId);
        }
    }

    class RejoinClusterRequestHandler implements TransportRequestHandler<RejoinClusterRequest> {
        @Override
        public void messageReceived(final RejoinClusterRequest request, final TransportChannel channel) throws Exception {
            try {
                channel.sendResponse(TransportResponse.Empty.INSTANCE);
            } catch (Exception e) {
                logger.warn("failed to send response on rejoin cluster request handling", e);
            }
            synchronized (stateMutex) {
                rejoin("received a request to rejoin the cluster from [" + request.fromNodeId + "]");
            }
        }
    }

    /**
     * All control of the join thread should happen under the cluster state update task thread.
     * This is important to make sure that the background joining process is always in sync with any cluster state updates
     * like master loss, failure to join, received cluster state while joining etc.
     */
    // NOTE:htt, 所有join线程的控制都应该在集群状态更新任务线程下进行。
    // 这一点很重要，以确保后台join进程始终与任何集群状态更新保持同步，例如主节点丢失、加入失败、加入时收到集群状态等。
    private class JoinThreadControl { // NOTE:htt, 控制join线程的启动、停止、标记完成等操作

        private final AtomicBoolean running = new AtomicBoolean(false); // NOTE:htt, 是否启动join线程
        private final AtomicReference<Thread> currentJoinThread = new AtomicReference<>(); // NOTE:htt, 当前join线程

        /** returns true if join thread control is started and there is currently an active join thread */
        // NOTE:htt, 返回true如果join线程控制已启动且当前有活跃的join线程
        public boolean joinThreadActive() { // NOTE:htt, 返回true如果join线程控制已启动且当前有活跃的join线程
            Thread currentThread = currentJoinThread.get(); // NOTE:htt, 当前join线程
            return running.get() && currentThread != null && currentThread.isAlive(); // NOTE:htt, 如果join线程控制已启动且当前有活跃的join线程，则返回true
        }   

        /** returns true if join thread control is started and the supplied thread is the currently active joinThread */
        // NOTE:htt, 返回true如果join线程控制已启动且供应的线程是当前活跃的join线程
        public boolean joinThreadActive(Thread joinThread) { // NOTE:htt, 返回true如果join线程控制已启动且供应的线程是当前活跃的join线程
            return running.get() && joinThread.equals(currentJoinThread.get()); // NOTE:htt, 如果join线程控制已启动且供应的线程是当前活跃的join线程，则返回true
        }

        /** cleans any running joining thread and calls {@link #rejoin} */
        // NOTE:htt, 停止任何活跃的join线程并调用{@link #rejoin}
        public void stopRunningThreadAndRejoin(String reason) { // NOTE:htt, 停止任何活跃的join线程并调用{@link #rejoin}
            assert Thread.holdsLock(stateMutex); // NOTE:htt, 确保当前线程持有stateMutex锁
            currentJoinThread.set(null); // NOTE:htt, 设置当前join线程为null
            rejoin(reason); // NOTE:htt, 重新加入集群
        }

        /** starts a new joining thread if there is no currently active one and join thread controlling is started */
        // NOTE:htt, 如果当前没有活跃的join线程且join线程控制已启动，则启动一个新的join线程
        public void startNewThreadIfNotRunning() { // NOTE:htt, 如果当前没有活跃的join线程且join线程控制已启动，则启动一个新的join线程
            assert Thread.holdsLock(stateMutex); // NOTE:htt, 确保当前线程持有stateMutex锁
            if (joinThreadActive()) { // NOTE:htt, 如果当前有活跃的join线程，则返回
                return; // NOTE:htt, 如果当前有活跃的join线程，则返回
            }
            threadPool.generic().execute(new Runnable() { // NOTE:htt, 执行新的join线程
                @Override
                public void run() {
                    Thread currentThread = Thread.currentThread(); // NOTE:htt, 当前线程
                    if (!currentJoinThread.compareAndSet(null, currentThread)) { // NOTE:htt, 如果当前join线程不为null，则返回
                        return; // NOTE:htt, 如果当前join线程不为null，则返回
                    }
                    while (running.get() && joinThreadActive(currentThread)) { // NOTE:htt, 如果join线程控制已启动且当前线程是活跃的join线程，则继续循环
                        try {
                            innerJoinCluster(); // NOTE:htt, 执行join操作
                            return; // NOTE:htt, 如果join操作成功，则返回
                        } catch (Exception e) {
                            logger.error("unexpected error while joining cluster, trying again", e); // NOTE:htt, 打印异常
                            // Because we catch any exception here, we want to know in
                            // tests if an uncaught exception got to this point and the test infra uncaught exception
                            // leak detection can catch this. In practise no uncaught exception should leak
                            // NOTE:htt, 因为这里捕获了任何异常，我们希望在测试中知道是否有未捕获的异常到达这一点，并且测试基础设施的未捕获异常泄漏检测可以捕获这一点。
                            // 在实践中，不应该有未捕获的异常到达这一点。
                            assert ExceptionsHelper.reThrowIfNotNull(e); // NOTE:htt, 如果异常不为null，则抛出异常
                        }
                    }
                    // cleaning the current thread from currentJoinThread is done by explicit calls.
                    // NOTE:htt, 从currentJoinThread中清理当前线程，通过显式调用完成
                }
            });
        }

        /**
         * marks the given joinThread as completed and makes sure another thread is running (starting one if needed)
         * If the given thread is not the currently running join thread, the command is ignored.
         */
        // NOTE:htt, 标记给定的join线程为完成，并确保另一个线程正在运行（如果需要则启动一个）。
        // 如果供应的线程不是当前活跃的join线程，则命令被忽略。
        public void markThreadAsDoneAndStartNew(Thread joinThread) { // NOTE:htt, 标记给定的join线程为完成，并确保另一个线程正在运行（如果需要则启动一个）。
            assert Thread.holdsLock(stateMutex); // NOTE:htt, 确保当前线程持有stateMutex锁
            if (!markThreadAsDone(joinThread)) { // NOTE:htt, 如果供应的线程不是当前活跃的join线程，则命令被忽略。
                return; // NOTE:htt, 如果供应的线程不是当前活跃的join线程，则命令被忽略。
            }
            startNewThreadIfNotRunning(); // NOTE:htt, 启动一个新的join线程
        }

        /** marks the given joinThread as completed. Returns false if the supplied thread is not the currently active join thread */
        public boolean markThreadAsDone(Thread joinThread) { // NOTE:htt, 标记给定的join线程为完成。如果供应的线程不是当前活跃的join线程，则返回false
            assert Thread.holdsLock(stateMutex); // NOTE:htt, 确保当前线程持有stateMutex锁
            return currentJoinThread.compareAndSet(joinThread, null); // NOTE:htt, 如果当前join线程与供应的线程相等，则设置为null，并返回true
        }

        public void stop() { // NOTE:htt, 停止join线程控制
            running.set(false); // NOTE:htt, 设置running为false
            Thread joinThread = currentJoinThread.getAndSet(null); // NOTE:htt, 获取当前join线程并设置为null
            if (joinThread != null) { // NOTE:htt, 如果当前join线程不为null，则中断
                joinThread.interrupt(); // NOTE:htt, 中断当前join线程
            }
        }

        public void start() { // NOTE:htt, 启动join线程控制
            running.set(true); // NOTE:htt, 设置running为true
        }

    }

    public final Collection<BiConsumer<DiscoveryNode, ClusterState>> getOnJoinValidators() {
        return onJoinValidators;
    }

}
