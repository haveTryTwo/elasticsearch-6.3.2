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

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.IncompatibleClusterStateVersionException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.compress.Compressor;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.discovery.AckClusterStatePublishResponseHandler;
import org.elasticsearch.discovery.BlockingClusterStatePublishResponseHandler;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BytesTransportRequest;
import org.elasticsearch.transport.EmptyTransportResponseHandler;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class PublishClusterStateAction extends AbstractComponent { // NOTE: htt, 选主成功后进入集群状态同步，这里涉及两个步骤： publish/send（需半数+1 master节点ack）, publish/commit
    // NOTE: htt, 其中整个阶段都是 publish，超时时间总共为30s，第一个阶段publish/send(超时时间为等待进入commit超时时间，即30s)，第二阶段为publish/commit(超时间为 publish超时时间30s - 之前publish/send使用时间)
    public static final String SEND_ACTION_NAME = "internal:discovery/zen/publish/send";    // NOTE: htt, send cluster state，为第一阶段（等待进入commit阶段，超时时间commitTimeout为30s）
    public static final String COMMIT_ACTION_NAME = "internal:discovery/zen/publish/commit"; // NOTE; htt, commit cluster state，为第二阶段（commit阶段，超时时间为 publishTimout - 之前publish/send使用时间)

    public interface IncomingClusterStateListener { // NOTE: htt, incoming cluster state and cluster state commit 状态进入时处理

        /**
         * called when a new incoming cluster state has been received.
         * Should validate the incoming state and throw an exception if it's not a valid successor state.
         */
        void onIncomingClusterState(ClusterState incomingState);

        /**
         * called when a cluster state has been committed and is ready to be processed
         */
        void onClusterStateCommitted(String stateUUID, ActionListener<Void> processedListener);
    }

    private final TransportService transportService;
    private final NamedWriteableRegistry namedWriteableRegistry;
    private final IncomingClusterStateListener incomingClusterStateListener; // NOTE: htt, incoming cluster state and cluster state commit 状态进入时处理
    private final DiscoverySettings discoverySettings; // NOTE: htt, discovery settings including noMasterBlock(默认无主是阻塞写，可以改配置) and publish/commit timeout

    private final AtomicLong fullClusterStateReceivedCount = new AtomicLong(); // NOTE: htt, 总共收到的clusterState的节点个数
    private final AtomicLong incompatibleClusterStateDiffReceivedCount = new AtomicLong(); // NOTE: htt, 不兼容diff集群状态个数
    private final AtomicLong compatibleClusterStateDiffReceivedCount = new AtomicLong(); // NOTE: htt, 兼容的通过diff获取全量集群状态的个数

    public PublishClusterStateAction(
            Settings settings,
            TransportService transportService,
            NamedWriteableRegistry namedWriteableRegistry,
            IncomingClusterStateListener incomingClusterStateListener,
            DiscoverySettings discoverySettings) {
        super(settings);
        this.transportService = transportService;
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.incomingClusterStateListener = incomingClusterStateListener;
        this.discoverySettings = discoverySettings;
        transportService.registerRequestHandler(SEND_ACTION_NAME, BytesTransportRequest::new, ThreadPool.Names.SAME, false, false,
            new SendClusterStateRequestHandler()); // NOTE: htt, 注册publish节点状态阶段，并且 在非主master节点上处理接受到新的clusterState
        transportService.registerRequestHandler(COMMIT_ACTION_NAME, CommitClusterStateRequest::new, ThreadPool.Names.SAME, false, false,
            new CommitClusterStateRequestHandler());
    }

    /**
     * publishes a cluster change event to other nodes. if at least minMasterNodes acknowledge the change it is committed and will
     * be processed by the master and the other nodes.
     * <p>
     * The method is guaranteed to throw a {@link org.elasticsearch.discovery.Discovery.FailedToCommitClusterStateException}
     * if the change is not committed and should be rejected.
     * Any other exception signals the something wrong happened but the change is committed.
     */
    public void publish(final ClusterChangedEvent clusterChangedEvent, final int minMasterNodes,
                        final Discovery.AckListener ackListener) throws Discovery.FailedToCommitClusterStateException {
        final DiscoveryNodes nodes;
        final SendingController sendingController; // NOTE:htt, 判断当前 publish 回包的master节点个数是否达到majority，如果已达到则在发送commit请求，并用 publishResponseHandler 处理publish请求失败情况
        final Set<DiscoveryNode> nodesToPublishTo; // NOTE:htt, 发送publish的节点列表
        final Map<Version, BytesReference> serializedStates; // NOTE: htt, full clusterState
        final Map<Version, BytesReference> serializedDiffs; // NOTE: htt, diff clusterState
        final boolean sendFullVersion; // NOTE: htt, 是否发送全量的clusterState
        try {
            nodes = clusterChangedEvent.state().nodes();
            nodesToPublishTo = new HashSet<>(nodes.getSize());
            DiscoveryNode localNode = nodes.getLocalNode();
            final int totalMasterNodes = nodes.getMasterNodes().size(); // NOTE: htt, master nodes
            for (final DiscoveryNode node : nodes) {
                if (node.equals(localNode) == false) { // NOTE: htt, 本地节点不发送请求
                    nodesToPublishTo.add(node);
                }
            }
            sendFullVersion = !discoverySettings.getPublishDiff() || clusterChangedEvent.previousState() == null; // NOTE: htt, 如果不允许publish diff的 clusterState或者为首次发布，则发送全量信息；
            serializedStates = new HashMap<>(); // NOTE:htt, 集群状态的全量值, <Version, 集群状态权利序列化后的二进制>
            serializedDiffs = new HashMap<>(); // NOTE:htt, 集群状态的diff值, <Version, diff序列化后二进制>

            // we build these early as a best effort not to commit in the case of error.
            // sadly this is not water tight as it may that a failed diff based publishing to a node
            // will cause a full serialization based on an older version, which may fail after the
            // change has been committed.
            buildDiffAndSerializeStates(clusterChangedEvent.state(), clusterChangedEvent.previousState(),
                    nodesToPublishTo, sendFullVersion, serializedStates, serializedDiffs); // NOTE:htt, 生成集群状态的diff(要么全量值，要么和prev state的diff),并和节点版本绑定

            final BlockingClusterStatePublishResponseHandler publishResponseHandler =
                new AckClusterStatePublishResponseHandler(nodesToPublishTo, ackListener); // NOTE:htt, 构建publish->commit阶段需要回包的节点列表
            sendingController = new SendingController(clusterChangedEvent.state(), minMasterNodes, // NOTE: htt, master节点最小确认个数default n/2+1
                totalMasterNodes, publishResponseHandler); // NOTE:htt, 判断当前 publish 回包的master节点个数是否达到majority，如果已达到则在发送commit请求，并用 publishResponseHandler
        } catch (Exception e) {
            throw new Discovery.FailedToCommitClusterStateException("unexpected error while preparing to publish", e);
        }

        try {
            innerPublish(clusterChangedEvent, nodesToPublishTo, sendingController, sendFullVersion, serializedStates, serializedDiffs); // NOTE:htt, 发送集群状态到所有节点，涉及 publish和commit两个阶段，publish需要majority master节点回包，然后进入commit阶段，这两个阶段在innerPublish中都会等待
        } catch (Discovery.FailedToCommitClusterStateException t) {
            throw t;
        } catch (Exception e) {
            // try to fail committing, in cause it's still on going
            if (sendingController.markAsFailed("unexpected error", e)) {
                // signal the change should be rejected
                throw new Discovery.FailedToCommitClusterStateException("unexpected error", e);
            } else {
                throw e;
            }
        }
    }

    private void innerPublish(final ClusterChangedEvent clusterChangedEvent, final Set<DiscoveryNode> nodesToPublishTo,
                              final SendingController sendingController, final boolean sendFullVersion,
                              final Map<Version, BytesReference> serializedStates, final Map<Version, BytesReference> serializedDiffs) { // NOTE:htt, 发送集群状态到所有节点，涉及 publish和commit两个阶段，publish需要majority master节点回包，然后进入commit阶段，这两个阶段在innerPublish中都会等待

        final ClusterState clusterState = clusterChangedEvent.state();
        final ClusterState previousState = clusterChangedEvent.previousState();
        final TimeValue publishTimeout = discoverySettings.getPublishTimeout(); // NOTE: htt, publish阶段的超时时间，默认30s

        final long publishingStartInNanos = System.nanoTime(); // NOTE:htt, publish开始时间

        for (final DiscoveryNode node : nodesToPublishTo) { // NOTE:htt, 将集群状态同步到其他的所有节点；其中分发到所有节点的state.nodes.masterNode和localNode都一样
            // try and serialize the cluster state once (or per version), so we don't serialize it
            // per node when we send it over the wire, compress it while we are at it...
            // we don't send full version if node didn't exist in the previous version of cluster state
            if (sendFullVersion || !previousState.nodes().nodeExists(node)) { // NOTE:htt, 发送全量集群状态，或者先前集群状态不包含该节点
                sendFullClusterState(clusterState, serializedStates, node, publishTimeout, sendingController); // NOTE:htt, 发送集群状态(pulish)到对应节点
            } else {
                sendClusterStateDiff(clusterState, serializedDiffs, serializedStates, node, publishTimeout, sendingController); // NOTE:htt, 发送diff集群状态(pulish)到对应节点
            }
        }

        sendingController.waitForCommit(discoverySettings.getCommitTimeout()); // NOTE: htt, 等待publish回包阶段（等待 n/2+1个master ack即可），并进入到发送commit阶段，或者超时(超时时间为30s)

        try { // NOTE: htt, 在publish/send 成功后进入 publish/commit阶段，如果有超时或者commit失败，则打印信息
            long timeLeftInNanos = Math.max(0, publishTimeout.nanos() - (System.nanoTime() - publishingStartInNanos)); // NOTE:htt, 判断离 publish超时剩余时间
            final BlockingClusterStatePublishResponseHandler publishResponseHandler = sendingController.getPublishResponseHandler();
            sendingController.setPublishingTimedOut(!publishResponseHandler.awaitAllNodes(TimeValue.timeValueNanos(timeLeftInNanos))); // NOTE:htt, 等待commit阶段全部回包或者超时
            if (sendingController.getPublishingTimedOut()) { // NOTE:htt, 如果commit阶段未收到全部节点的回包
                DiscoveryNode[] pendingNodes = publishResponseHandler.pendingNodes(); // NOTE:htt, 获取未收到commit回包的列表
                // everyone may have just responded
                if (pendingNodes.length > 0) { // NOTE: htt, 打印commit阶段未收到回包的节点信息
                    logger.warn("timed out waiting for all nodes to process published state [{}] (timeout [{}], pending nodes: {})",
                        clusterState.version(), publishTimeout, pendingNodes);
                }
            }
            // The failure is logged under debug when a sending failed. we now log a summary.
            Set<DiscoveryNode> failedNodes = publishResponseHandler.getFailedNodes(); // NOTE:htt, publish 或 commit 阶段失败的节点
            if (failedNodes.isEmpty() == false) { // NOTE:htt, 打印失败节点信息
                logger.warn("publishing cluster state with version [{}] failed for the following nodes: [{}]",
                    clusterChangedEvent.state().version(), failedNodes);
            }
        } catch (InterruptedException e) {
            // ignore & restore interrupt
            Thread.currentThread().interrupt();
        }
    }

    private void buildDiffAndSerializeStates(ClusterState clusterState, ClusterState previousState, Set<DiscoveryNode> nodesToPublishTo,
                                             boolean sendFullVersion, Map<Version, BytesReference> serializedStates,
                                             Map<Version, BytesReference> serializedDiffs) { // NOTE:htt, 生成集群状态的diff(要么全量值，要么和prev state的diff),并和节点版本绑定
        Diff<ClusterState> diff = null;
        for (final DiscoveryNode node : nodesToPublishTo) {
            try {
                if (sendFullVersion || !previousState.nodes().nodeExists(node)) { // NOTE:htt, 发送全量集群状态，或者先前集群状态不包含该节点
                    // will send a full reference
                    if (serializedStates.containsKey(node.getVersion()) == false) { // NOTE:htt, 当前Version没有加入到状态中，则添加该Version和对应集群全量的状态
                        serializedStates.put(node.getVersion(), serializeFullClusterState(clusterState, node.getVersion())); // NOTE:htt, 添加<节点Version, 全量集群状态序列化后的二进制信息>
                    }
                } else {
                    // will send a diff
                    if (diff == null) {
                        diff = clusterState.diff(previousState);  // NOTE: htt, 获取集群状态的diff
                    }
                    if (serializedDiffs.containsKey(node.getVersion()) == false) {
                        serializedDiffs.put(node.getVersion(), serializeDiffClusterState(diff, node.getVersion())); // NOTE: htt, 设置对应version的集群状态的diff
                    }
                }
            } catch (IOException e) {
                throw new ElasticsearchException("failed to serialize cluster_state for publishing to node {}", e, node);
            }
        }
    }

    private void sendFullClusterState(ClusterState clusterState, Map<Version, BytesReference> serializedStates,
                                      DiscoveryNode node, TimeValue publishTimeout, SendingController sendingController) { // NOTE:htt, 发送全量集群状态(pulish)到对应节点
        BytesReference bytes = serializedStates.get(node.getVersion());
        if (bytes == null) { // NOTE:htt, 如果没有找到对应Versiond的全量的集群状态，则重新生成下
            try {
                bytes = serializeFullClusterState(clusterState, node.getVersion());
                serializedStates.put(node.getVersion(), bytes); // NOTE: htt, put <version, clusterState.serializeFullString>
            } catch (Exception e) {
                logger.warn(() -> new ParameterizedMessage("failed to serialize cluster_state before publishing it to node {}", node), e);
                sendingController.onNodeSendFailed(node, e);
                return;
            }
        }
        sendClusterStateToNode(clusterState, bytes, node, publishTimeout, sendingController, false, serializedStates); // NOTE:htt, 发送全量集群状态(pulish)到对应节点
    }

    private void sendClusterStateDiff(ClusterState clusterState,
                                      Map<Version, BytesReference> serializedDiffs, Map<Version, BytesReference> serializedStates,
                                      DiscoveryNode node, TimeValue publishTimeout, SendingController sendingController) { // NOTE:htt, 发送diff集群状态(pulish)到对应节点
        BytesReference bytes = serializedDiffs.get(node.getVersion());
        assert bytes != null : "failed to find serialized diff for node " + node + " of version [" + node.getVersion() + "]";
        sendClusterStateToNode(clusterState, bytes, node, publishTimeout, sendingController, true, serializedStates); // NOTE:htt, 发送diff集群状态(pulish)到对应节点
    }

    private void sendClusterStateToNode(final ClusterState clusterState, BytesReference bytes,
                                        final DiscoveryNode node,
                                        final TimeValue publishTimeout,
                                        final SendingController sendingController,
                                        final boolean sendDiffs, final Map<Version, BytesReference> serializedStates) { // NOTE:htt, 发送集群状态(pulish)到对应节点
        try {

            // -> no need to put a timeout on the options here, because we want the response to eventually be received
            //  and not log an error if it arrives after the timeout
            // -> no need to compress, we already compressed the bytes
            TransportRequestOptions options = TransportRequestOptions.builder()
                .withType(TransportRequestOptions.Type.STATE).withCompress(false).build(); // NOTE: htt, 发送集群状态
            transportService.sendRequest(node, SEND_ACTION_NAME, // NOTE: htt, 发送pulish请求（发送集群状态），会一直等待
                    new BytesTransportRequest(bytes, node.getVersion()),
                    options,
                    new EmptyTransportResponseHandler(ThreadPool.Names.SAME) {

                        @Override
                        public void handleResponse(TransportResponse.Empty response) {
                            if (sendingController.getPublishingTimedOut()) {
                                logger.debug("node {} responded for cluster state [{}] (took longer than [{}])", node,
                                    clusterState.version(), publishTimeout);
                            }
                            sendingController.onNodeSendAck(node); // NOTE:htt, 节点收到pulish回包后处理，如果收到master节点请求回包过半，则进入commit阶段并发送commit请求
                        }

                        @Override
                        public void handleException(TransportException exp) {
                            if (sendDiffs && exp.unwrapCause() instanceof IncompatibleClusterStateVersionException) { // NOTE: htt, 如果send diff集群状态因对应节点没有原有的集群状态数据，那么就发送全量信息
                                logger.debug("resending full cluster state to node {} reason {}", node, exp.getDetailedMessage());
                                sendFullClusterState(clusterState, serializedStates, node, publishTimeout, sendingController); // NOTE:htt, 发送全量集群状态(pulish)到对应节点
                            } else {
                                logger.debug(() -> new ParameterizedMessage("failed to send cluster state to {}", node), exp);
                                sendingController.onNodeSendFailed(node, exp); // NOTE: htt, publish节点回包failed
                            }
                        }
                    });
        } catch (Exception e) {
            logger.warn(() -> new ParameterizedMessage("error sending cluster state to {}", node), e);
            sendingController.onNodeSendFailed(node, e);
        }
    }

    private void sendCommitToNode(final DiscoveryNode node, final ClusterState clusterState, final SendingController sendingController) { // NOTE:htt, 发送commit请求到节点
        try {
            logger.trace("sending commit for cluster state (uuid: [{}], version [{}]) to [{}]",
                clusterState.stateUUID(), clusterState.version(), node);
            TransportRequestOptions options = TransportRequestOptions.builder().withType(TransportRequestOptions.Type.STATE).build(); // NOTE: htt, 设置发送type为STATE
            // no need to put a timeout on the options here, because we want the response to eventually be received
            // and not log an error if it arrives after the timeout
            transportService.sendRequest(node, COMMIT_ACTION_NAME, // NOTE: htt, send commit请求，并且会一直等待
                    new CommitClusterStateRequest(clusterState.stateUUID()),
                    options,
                    new EmptyTransportResponseHandler(ThreadPool.Names.SAME) {

                        @Override
                        public void handleResponse(TransportResponse.Empty response) {
                            if (sendingController.getPublishingTimedOut()) { // NOTE:htt, 如果publishing超时，则记录日志
                                logger.debug("node {} responded to cluster state commit [{}]", node, clusterState.version());
                            }
                            sendingController.getPublishResponseHandler().onResponse(node); // NOTE: htt, 仅commit请求完成后，则认为该节点publish/commit处理完成
                        }

                        @Override
                        public void handleException(TransportException exp) {
                            logger.debug(() -> new ParameterizedMessage("failed to commit cluster state (uuid [{}], version [{}]) to {}",
                                    clusterState.stateUUID(), clusterState.version(), node), exp);
                            sendingController.getPublishResponseHandler().onFailure(node, exp); // NOTE:htt, commit请求失败，则认为该节点publish处理失败
                        }
                    });
        } catch (Exception t) {
            logger.warn(() -> new ParameterizedMessage("error sending cluster state commit (uuid [{}], version [{}]) to {}",
                    clusterState.stateUUID(), clusterState.version(), node), t);
            sendingController.getPublishResponseHandler().onFailure(node, t); // NOTE:htt, 发送异常，则认为该接节点publish异常
        }
    }


    public static BytesReference serializeFullClusterState(ClusterState clusterState, Version nodeVersion) throws IOException { // NOTE:htt, 获取全量集群状态序列化后的二进制信息
        BytesStreamOutput bStream = new BytesStreamOutput();
        try (StreamOutput stream = CompressorFactory.COMPRESSOR.streamOutput(bStream)) {
            stream.setVersion(nodeVersion); // NOTE:htt, 设置节点ES版本
            stream.writeBoolean(true); //NOTE:htt, 全量的集群状态的标记
            clusterState.writeTo(stream); // NOTE:htt, 记录全量的集群状态
        }
        return bStream.bytes(); // NOTE:htt, 获取序列化后的二进制
    }

    public static BytesReference serializeDiffClusterState(Diff diff, Version nodeVersion) throws IOException { // NOTE:htt, 获取diff的集群状态的二进制信息
        BytesStreamOutput bStream = new BytesStreamOutput();
        try (StreamOutput stream = CompressorFactory.COMPRESSOR.streamOutput(bStream)) {
            stream.setVersion(nodeVersion);  // NOTE:htt, 设置节点ES版本
            stream.writeBoolean(false); //NOTE:htt, diff集群状态的标记
            diff.writeTo(stream); // NOTE:htt, 将集群状态的diff内容写入到stream中
        }
        return bStream.bytes(); // NOTE:htt, 获取序列化后的二进制
    }

    private Object lastSeenClusterStateMutex = new Object(); // NOTE:htt, 对集群状态的应用加锁
    private ClusterState lastSeenClusterState; // NOTE:htt, 最后一次收到集群状态

    protected void handleIncomingClusterStateRequest(BytesTransportRequest request, TransportChannel channel) throws IOException { // NOTE:htt, pulish请求处理，读取集群状态(组合成新的集群状态),并保存到队列中(待收到master的commit请求在执行更新),然后回复pulish请求
        Compressor compressor = CompressorFactory.compressor(request.bytes());
        StreamInput in = request.bytes().streamInput();
        final ClusterState incomingState;
        synchronized (lastSeenClusterStateMutex) {
            try {
                if (compressor != null) {
                    in = compressor.streamInput(in);
                }
                in = new NamedWriteableAwareStreamInput(in, namedWriteableRegistry);
                in.setVersion(request.version());
                // If true we received full cluster state - otherwise diffs
                if (in.readBoolean()) { // NOTE:htt, 收取到全量集群状态信息
                    incomingState = ClusterState.readFrom(in, transportService.getLocalNode()); // NOTE:htt, 读取集群状态序列化信息
                    fullClusterStateReceivedCount.incrementAndGet(); // NOTE: htt, 总共收到的clusterState的节点个数
                    logger.debug("received full cluster state version [{}] with size [{}]", incomingState.version(),
                        request.bytes().length());
                } else if (lastSeenClusterState != null) { // NOTE:htt, 通过diff数据合并成全量数据，需要先有之前的集群状态数据
                    Diff<ClusterState> diff = ClusterState.readDiffFrom(in, lastSeenClusterState.nodes().getLocalNode()); // NOTE:htt, 读取序列化diff数据
                    incomingState = diff.apply(lastSeenClusterState); // NOTE:htt, 根据从stream获取的diff数据，以及当前的state数据，整合最新的集群状态
                    compatibleClusterStateDiffReceivedCount.incrementAndGet(); // NOTE: htt, 兼容的通过diff获取全量集群状态的个数
                    logger.debug("received diff cluster state version [{}] with uuid [{}], diff size [{}]",
                        incomingState.version(), incomingState.stateUUID(), request.bytes().length());
                } else {
                    logger.debug("received diff for but don't have any local cluster state - requesting full state");
                    throw new IncompatibleClusterStateVersionException("have no local cluster state"); // NOTE:htt, diff时没有原有的集群状态(比如节点重启)
                }
            } catch (IncompatibleClusterStateVersionException e) {
                incompatibleClusterStateDiffReceivedCount.incrementAndGet(); // NOTE: htt, 不兼容diff集群状态个数
                throw e;
            } catch (Exception e) {
                logger.warn("unexpected error while deserializing an incoming cluster state", e);
                throw e;
            } finally {
                IOUtils.close(in);
            }
            incomingClusterStateListener.onIncomingClusterState(incomingState); // NOTE:htt, zenPing.onIncomingClusterState() 添加新的集群状态到队列中，待收到master的commit请求后再执行集群状态更新
            lastSeenClusterState = incomingState; // NOTE: htt, 更新最新的集群状态
        }
        channel.sendResponse(TransportResponse.Empty.INSTANCE); // NOTE:htt, 回复publish包
    }

    protected void handleCommitRequest(CommitClusterStateRequest request, final TransportChannel channel) {
        incomingClusterStateListener.onClusterStateCommitted(request.stateUUID, new ActionListener<Void>() {

            @Override
            public void onResponse(Void ignore) {
                try {
                    // send a response to the master to indicate that this cluster state has been processed post committing it.
                    channel.sendResponse(TransportResponse.Empty.INSTANCE); // NOTE: htt, 集群状态本地更新后，回包给master节点
                } catch (Exception e) {
                    logger.debug("failed to send response on cluster state processed", e);
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                try {
                    channel.sendResponse(e); // NOTE: htt, 恢复本地状态更新异常
                } catch (Exception inner) {
                    inner.addSuppressed(e);
                    logger.debug("failed to send response on cluster state processed", inner);
                }
            }
        });
    }

    private class SendClusterStateRequestHandler implements TransportRequestHandler<BytesTransportRequest> { // NOTE:htt, pulish请求处理，读取集群状态(组合成新的集群状态),并保存到队列中(待收到master的commit请求在执行更新),然后回复pulish请求

        @Override
        public void messageReceived(BytesTransportRequest request, final TransportChannel channel) throws Exception {
            handleIncomingClusterStateRequest(request, channel); // NOTE:htt, pulish请求处理，读取集群状态(组合成新的集群状态),并保存到队列中(待收到master的commit请求在执行更新),然后回复pulish请求
        }
    }

    private class CommitClusterStateRequestHandler implements TransportRequestHandler<CommitClusterStateRequest> { // NOTE: htt, handle commit request of master
        @Override
        public void messageReceived(CommitClusterStateRequest request, final TransportChannel channel) throws Exception {
            handleCommitRequest(request, channel);
        }
    }

    protected static class CommitClusterStateRequest extends TransportRequest { // NOTE: htt, commit 集群状态，包括 stateUUID

        String stateUUID; // NOTE: htt, 集群状态的uuid state uuid

        public CommitClusterStateRequest() {
        }

        public CommitClusterStateRequest(String stateUUID) {
            this.stateUUID = stateUUID;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            stateUUID = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(stateUUID);
        }
    }


    /**
     * Coordinates acknowledgments of the sent cluster state from the different nodes. Commits the change
     * after `minimum_master_nodes` have successfully responded or fails the entire change. After committing
     * the cluster state, will trigger a commit message to all nodes that responded previously and responds immediately
     * to all future acknowledgments.
     */
    class SendingController { // NOTE:htt, 判断当前 publish 回包的master节点个数是否达到majority，如果已达到则在发送commit请求，并用 publishResponseHandler 处理publish请求失败情况

        private final ClusterState clusterState; // NOTE:htt, 集群状态

        public BlockingClusterStatePublishResponseHandler getPublishResponseHandler() {
            return publishResponseHandler; // NOTE:htt, 返回publishResponseHandler，在外部处理收包后情况
        }

        private final BlockingClusterStatePublishResponseHandler publishResponseHandler;// NOTE: htt, 等待publish 列表节点回包（包括publish和commit请求回包），如果收到全部回包或一段时间没有回包则返回
        final ArrayList<DiscoveryNode> sendAckedBeforeCommit = new ArrayList<>(); // NOTE:htt, commit 前已经收到回包的节点请求

        // writes and reads of these are protected under synchronization
        final CountDownLatch committedOrFailedLatch; // 0 count indicates that a decision was made w.r.t committing or failing
        boolean committed;  // true if cluster state was committed // NOTE:htt, 集群状态是否已经commit
        int neededMastersToCommit; // number of master nodes acks still needed before committing, // NOTE: htt, master节点必须ack的个数(一般为n/2+1)，这样才能commit
        int pendingMasterNodes; // how many master node still need to respond, // NOTE:htt, 需要回复的节点数

        // an external marker to note that the publishing process is timed out. This is useful for proper logging.
        final AtomicBoolean publishingTimedOut = new AtomicBoolean();

        private SendingController(ClusterState clusterState, int minMasterNodes, int totalMasterNodes,
                                  BlockingClusterStatePublishResponseHandler publishResponseHandler) {
            this.clusterState = clusterState;
            this.publishResponseHandler = publishResponseHandler;
            this.neededMastersToCommit = Math.max(0, minMasterNodes - 1); // we are one of the master nodes  // NOTE: htt, ack的master节点数，去掉自己(当前也是master节点)，并且只需要考虑最小master ack的个数
            this.pendingMasterNodes = totalMasterNodes - 1;
            if (this.neededMastersToCommit > this.pendingMasterNodes) {
                throw new Discovery.FailedToCommitClusterStateException("not enough masters to ack sent cluster state." +
                    "[{}] needed , have [{}]", neededMastersToCommit, pendingMasterNodes);
            }
            this.committed = neededMastersToCommit == 0;
            this.committedOrFailedLatch = new CountDownLatch(committed ? 0 : 1);
        }

        public void waitForCommit(TimeValue commitTimeout) { // NOTE: htt,等待去commit，预计30s超时，以期望能够进行commit
            boolean timedout = false;
            try {
                timedout = committedOrFailedLatch.await(commitTimeout.millis(), TimeUnit.MILLISECONDS) == false; // NOTE:htt, 等待直到完成或超时 (等待minMasterNodes - 1 个 master 节点ack)
            } catch (InterruptedException e) {
                // the commit check bellow will either translate to an exception or we are committed and we can safely continue
            }

            if (timedout) {  // NOTE:htt, 设置publish阶段为失败
                markAsFailed("timed out waiting for commit (commit timeout [" + commitTimeout + "])");
            }
            if (isCommitted() == false) { // NOTE: htt, 如果不能进行commit状态（publish/send没有半数成功）则直接抛出失败
                throw new Discovery.FailedToCommitClusterStateException("{} enough masters to ack sent cluster state. [{}] left",
                        timedout ? "timed out while waiting for" : "failed to get", neededMastersToCommit);
            }
        }

        public synchronized boolean isCommitted() {
            return committed;
        }

        public synchronized void onNodeSendAck(DiscoveryNode node) { // NOTE:htt, 节点收到pulish回包后处理，如果收到master节点请求回包过半，则进入commit阶段并发送commit请求
            if (committed) { // NOTE:htt, 如果已经committed，则直接发送commit请求
                assert sendAckedBeforeCommit.isEmpty();
                sendCommitToNode(node, clusterState, this); // NOTE: htt, 如果已经为可以commit阶段，则直接发送 commit 信息
            } else if (committedOrFailed()) { // NOTE:htt, 如果commit 为false，但是committedOrFailedLatch已设置，则说明超时，不用处理请求
                logger.trace("ignoring ack from [{}] for cluster state version [{}]. already failed", node, clusterState.version());
            } else { // NOTE:htt, 等待中，则将节点加入到待发送commit请求的列表中
                // we're still waiting
                sendAckedBeforeCommit.add(node);
                if (node.isMasterNode()) { // NOTE:htt, 对master节点进行判断，用于判断是否超过半数
                    checkForCommitOrFailIfNoPending(node); // NOTE:htt, 减少等待的master回包数，如果收到过半回包则发送commit请求
                }
            }
        }

        private synchronized boolean committedOrFailed() { // NOTE:htt, 判断commit等待的CountDown是否已经为0，为0则已经完成commit
            return committedOrFailedLatch.getCount() == 0;
        }

        /**
         * check if enough master node responded to commit the change. fails the commit
         * if there are no more pending master nodes but not enough acks to commit.
         */
        private synchronized void checkForCommitOrFailIfNoPending(DiscoveryNode masterNode) { // NOTE:htt, 减少等待的master回包数，如果收到过半回包则发送commit请求
            logger.trace("master node {} acked cluster state version [{}]. processing ... (current pending [{}], needed [{}])",
                    masterNode, clusterState.version(), pendingMasterNodes, neededMastersToCommit);
            neededMastersToCommit--;
            if (neededMastersToCommit == 0) { // NOTE:htt, 如果满足半数以上master节点回包，则认为成功
                if (markAsCommitted()) { // NOTE: htt, 所有的 needMasterToCommit 节点已经回复（majority master已回复），则进入commit状态，往已恢复pulish的节点发送commit请求
                    for (DiscoveryNode nodeToCommit : sendAckedBeforeCommit) {
                        sendCommitToNode(nodeToCommit, clusterState, this); // NOTE:htt, 发送commit请求到节点
                    }
                    sendAckedBeforeCommit.clear();
                }
            }
            decrementPendingMasterAcksAndChangeForFailure(); // NOTE:htt, 减少pending的master个数，并判断是否失败
        }

        private synchronized void decrementPendingMasterAcksAndChangeForFailure() { // NOTE:htt, 减少pending的master个数，并判断是否失败
            pendingMasterNodes--;
            if (pendingMasterNodes == 0 && neededMastersToCommit > 0) { // NOTE:htt, 如果pendingmaster已减为0，但是没有收到半数以上master回包，则返回出错
                markAsFailed("no more pending master nodes, but failed to reach needed acks ([" + neededMastersToCommit + "] left)");
            }
        }

        public synchronized void onNodeSendFailed(DiscoveryNode node, Exception e) { // NOTE:htt, 节点请求失败处理
            if (node.isMasterNode()) { // NOTE:htt, 如果是master，则减少总的master待回包数，以便判断是否大多数master已经回包
                logger.trace("master node {} failed to ack cluster state version [{}]. " +
                        "processing ... (current pending [{}], needed [{}])",
                        node, clusterState.version(), pendingMasterNodes, neededMastersToCommit);
                decrementPendingMasterAcksAndChangeForFailure(); // NOTE:htt, 减少pending的master个数，并判断是否失败
            }
            publishResponseHandler.onFailure(node, e); // NOTE: htt, publish阶段失败，则认为Publish/commit阶段都失败处理
        }

        /**
         * tries and commit the current state, if a decision wasn't made yet
         *
         * @return true if successful
         */
        private synchronized boolean markAsCommitted() { // NOTE:htt, 标记当前当前已经commit
            if (committedOrFailed()) {
                return committed;  // NOTE: htt, 如果committedOrFailedLatch ==0，则表明当前 committed已经设置，要么为true，要么为false 
            }
            logger.trace("committing version [{}]", clusterState.version());
            committed = true; // NOTE: htt, make committed true
            committedOrFailedLatch.countDown();
            return true;
        }

        /**
         * tries marking the publishing as failed, if a decision wasn't made yet
         *
         * @return true if the publishing was failed and the cluster state is *not* committed
         **/
        private synchronized boolean markAsFailed(String details, Exception reason) { // NOTE:htt, 设置失败
            if (committedOrFailed()) { // NOTE: htt, 如果committedOrFailedLatch ==0，则表明当前 committed已经设置，要么为true，要么为false 
                return committed == false;
            }
            logger.trace(() -> new ParameterizedMessage("failed to commit version [{}]. {}",
                clusterState.version(), details), reason);
            committed = false;
            committedOrFailedLatch.countDown();
            return true;
        }

        /**
         * tries marking the publishing as failed, if a decision wasn't made yet
         *
         * @return true if the publishing was failed and the cluster state is *not* committed
         **/
        private synchronized boolean markAsFailed(String reason) { // NOTE:htt, 设置为失败
            if (committedOrFailed()) { // NOTE: htt, 如果committedOrFailedLatch ==0，则表明当前 committed已经设置，要么为true，要么为false 
                return committed == false;
            }
            logger.trace("failed to commit version [{}]. {}", clusterState.version(), reason);
            committed = false; // NOTE: htt, not committed
            committedOrFailedLatch.countDown(); // NOTE:htt, 减少countDown
            return true;
        }

        public boolean getPublishingTimedOut() {
            return publishingTimedOut.get();
        }

        public void setPublishingTimedOut(boolean isTimedOut) { // NOTE:htt, 外部设置publishing已经超时
            publishingTimedOut.set(isTimedOut);
        }
    }

    public PublishClusterStateStats stats() { // NOTE: htt, 节点收到master集群状态一致性统计
        return new PublishClusterStateStats(
            fullClusterStateReceivedCount.get(),
            incompatibleClusterStateDiffReceivedCount.get(),
            compatibleClusterStateDiffReceivedCount.get());
    }
}
