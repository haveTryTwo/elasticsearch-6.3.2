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
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;

import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentMap;

/**
 * A fault detection of multiple nodes.
 */
public class NodesFaultDetection extends FaultDetection { // NOTE: htt, 在主master节点上的心跳 探测，探测的目标是其他所有的节点，如果存活则延迟1s继续探测，否则30s超时,并连续3次失败(共90s)则移除节点；如果是端口连接则立即删除节点（此时不重试）

    public static final String PING_ACTION_NAME = "internal:discovery/zen/fd/ping";

    public abstract static class Listener { // NOTE: htt, 对非mater节点探测失败后的处理

        public void onNodeFailure(DiscoveryNode node, String reason) {}

        public void onPingReceived(PingRequest pingRequest) {}

    }

    private final CopyOnWriteArrayList<Listener> listeners = new CopyOnWriteArrayList<>(); // NOTE: htt, 非master节点探测失败后的监听处理, listener on fault detection

    private final ConcurrentMap<DiscoveryNode, NodeFD> nodesFD = newConcurrentMap(); // NOTE: htt,待所有探测的非master节点, all nodesFD(not including main master) which key is node, value is nodeFD
                                                                                     // NOTE:htt, 加入到探测列表中，在NodeFD探测前会检查是否在nodesFD列表中，如果不在就不继续探测
    private final Supplier<ClusterState> clusterStateSupplier; // NOTE:htt, 当前master节点的state

    private volatile DiscoveryNode localNode; // NOTE: htt, 当前节点，即master节点

    public NodesFaultDetection(Settings settings, ThreadPool threadPool, TransportService transportService,
                               Supplier<ClusterState> clusterStateSupplier, ClusterName clusterName) {
        super(settings, threadPool, transportService, clusterName);

        this.clusterStateSupplier = clusterStateSupplier;

        logger.debug("[node  ] uses ping_interval [{}], ping_timeout [{}], ping_retries [{}]", pingInterval, pingRetryTimeout,
            pingRetryCount);

        transportService.registerRequestHandler( // NOTE: htt, 注册 PingRequestHandler() 用于在非master节点上处理master的心跳请求
            PING_ACTION_NAME, PingRequest::new, ThreadPool.Names.SAME, false, false, new PingRequestHandler());
    }

    public void setLocalNode(DiscoveryNode localNode) {
        this.localNode = localNode;
    }

    public void addListener(Listener listener) {
        listeners.add(listener);
    }

    public void removeListener(Listener listener) {
        listeners.remove(listener);
    }

    /**
     * Gets the current set of nodes involved in node fault detection.
     * NB: For testing purposes.
     */
    public Set<DiscoveryNode> getNodes() {
        return Collections.unmodifiableSet(nodesFD.keySet());
    }

    /**
     * make sure that nodes in clusterState are pinged. Any pinging to nodes which are not
     * part of the cluster will be stopped
     */
    public void updateNodesAndPing(ClusterState clusterState) { // NOTE: htt, 如果探测节点不在集群列表中则移除；如果集群列表中节点不在探测列表中则加入探测
        // remove any nodes we don't need, this will cause their FD to stop
        for (DiscoveryNode monitoredNode : nodesFD.keySet()) {
            if (!clusterState.nodes().nodeExists(monitoredNode)) { // NOTE:htt, 如果当前探测节点不在集群节点列表中，则将节点从探测中移除
                nodesFD.remove(monitoredNode);
            }
        }
        // add any missing nodes

        for (DiscoveryNode node : clusterState.nodes()) {
            if (node.equals(localNode)) {
                // no need to monitor the local node
                continue;
            }
            if (!nodesFD.containsKey(node)) { // NOTE:htt, 如果探测节点列表中不包含集群中节点，则将该节点加入到探测中
                NodeFD fd = new NodeFD(node);
                // it's OK to overwrite an existing nodeFD - it will just stop and the new one will pick things up.
                nodesFD.put(node, fd); // NOTE:htt, 加入到探测列表中，在NodeFD探测前会检查是否在nodesFD列表中，如果不在就不继续探测
                // we use schedule with a 0 time value to run the pinger on the pool as it will run on later
                threadPool.schedule(TimeValue.timeValueMillis(0), ThreadPool.Names.SAME, fd); // NOTE:htt, 对新加入的节点立即启用探测; 多个节点是并行探测的；
            }
        }
    }

    /** stops all pinging **/
    public NodesFaultDetection stop() { // NOTE:htt, 停止探测，在NodeFD探测前会检查是否在nodesFD列表中，如果不在就不继续探测
        nodesFD.clear(); // NOTE:htt, 清空nodesFD列表，则NodeFD探测前就发现不在其中，即不会继续探测
        return this;
    }

    @Override
    public void close() {
        super.close();
        stop();
    }

    @Override
    protected void handleTransportDisconnect(DiscoveryNode node) {
        NodeFD nodeFD = nodesFD.remove(node); // NOTE:htt, 从待探测节点中移除
        if (nodeFD == null) {
            return;
        }
        if (connectOnNetworkDisconnect) { // NOTE: htt, 连接断开时重新连接，默认为false which default is false
            NodeFD fd = new NodeFD(node);
            try {
                transportService.connectToNode(node); // NOTE:htt, 尝试连接节点
                nodesFD.put(node, fd); // NOTE:htt, 将节点添加到待探测列表中
                // we use schedule with a 0 time value to run the pinger on the pool as it will run on later
                threadPool.schedule(TimeValue.timeValueMillis(0), ThreadPool.Names.SAME, fd); // NOTE:htt, 立即发送往非master节点心跳
            } catch (Exception e) {
                logger.trace("[node  ] [{}] transport disconnected (with verified connect)", node);
                // clean up if needed, just to be safe..
                nodesFD.remove(node, fd); // NOTE:htt 从待探测节点中移除
                notifyNodeFailure(node, "transport disconnected (with verified connect)");
            }
        } else {
            logger.trace("[node  ] [{}] transport disconnected", node);
            notifyNodeFailure(node, "transport disconnected");
        }
    }

    private void notifyNodeFailure(final DiscoveryNode node, final String reason) { // NOTE:htt, 通知节点node失败
        try {
            threadPool.generic().execute(new Runnable() {
                @Override
                public void run() {
                    for (Listener listener : listeners) {
                        listener.onNodeFailure(node, reason); // NOTE: htt, do batch failure listener if ping non-master node failed
                    }
                }
            });
        } catch (EsRejectedExecutionException ex) {
            logger.trace(() -> new ParameterizedMessage(
                    "[node  ] [{}] ignoring node failure (reason [{}]). Local node is shutting down", node, reason), ex);
        }
    }

    private void notifyPingReceived(final PingRequest pingRequest) {
        threadPool.generic().execute(new Runnable() {

            @Override
            public void run() {
                for (Listener listener : listeners) {
                    listener.onPingReceived(pingRequest); // NOTE: htt, 非master节点处理master节点心跳探测请求
                }
            }

        });
    }


    private class NodeFD implements Runnable { // NOTE: htt, (在主master上进行对非主master节点的心跳探测)，对每个非master都是一个独立的run()检查
        volatile int retryCount; // NOTE:htt, 重试次数

        private final DiscoveryNode node; // NOTE: htt, 目标节点

        private NodeFD(DiscoveryNode node) {
            this.node = node;
        }

        private boolean running() {
            return NodeFD.this.equals(nodesFD.get(node)); // NOTE:htt, 判断当前引用的Node是否在nodesFD中，如果stop会清理nodesFD中的nodes
        }

        private PingRequest newPingRequest() {
            return new PingRequest(node, clusterName, localNode, clusterStateSupplier.get().version());
        }

        @Override
        public void run() {
            if (!running()) { // NOTE:htt, 判断当前Node是否在nodesFD，如果不在就不用继续探测心跳
                return;
            }
            final TransportRequestOptions options = TransportRequestOptions.builder().withType(TransportRequestOptions.Type.PING) // NOTE: htt, ping non master node
                .withTimeout(pingRetryTimeout).build(); // NOTE: htt, ping 超时时间为 30 seconds
            transportService.sendRequest(node, PING_ACTION_NAME, newPingRequest(), options, new TransportResponseHandler<PingResponse>() {
                        @Override
                        public PingResponse newInstance() {
                            return new PingResponse();
                        }

                        @Override
                        public void handleResponse(PingResponse response) {
                            if (!running()) {
                                return;
                            }
                            retryCount = 0; // NOTE:htt, 心跳探测重试设置为0，3次超时探测中，只要有1次探测成功则重置探测次数
                            threadPool.schedule(pingInterval, ThreadPool.Names.SAME, NodeFD.this); // NOTE: htt, 启动下一次心跳探测，时间间隔为1s
                        }

                        @Override
                        public void handleException(TransportException exp) {
                            if (!running()) {
                                return;
                            }
                            if (exp instanceof ConnectTransportException || exp.getCause() instanceof ConnectTransportException) {
                                handleTransportDisconnect(node); // NOTE:htt, 处理非master节点连接异常
                                return;
                            }

                            retryCount++; // NOTE: htt, 增加重试的次数，每次超时时间30s
                            logger.trace( () -> new ParameterizedMessage(
                                    "[node  ] failed to ping [{}], retry [{}] out of [{}]", node, retryCount, pingRetryCount), exp);
                            if (retryCount >= pingRetryCount) { // NOTE: htt, 连续超过3次，每次30s，共90s，则通知非master节点失败，
                                logger.debug("[node  ] failed to ping [{}], tried [{}] times, each with  maximum [{}] timeout", node,
                                    pingRetryCount, pingRetryTimeout);
                                // not good, failure
                                if (nodesFD.remove(node, NodeFD.this)) { // NOTE: htt, 从待探测节点列表中移除, remove this node which has timeout
                                    notifyNodeFailure(node, "failed to ping, tried [" + pingRetryCount + "] times, each with maximum ["
                                        + pingRetryTimeout + "] timeout"); // NOTE:htt, 通知节点node失败
                                }
                            } else {
                                // resend the request, not reschedule, rely on send timeout
                                transportService.sendRequest(node, PING_ACTION_NAME, newPingRequest(), options, this);  // NOTE: htt, 当未到达3次，则立即继续发送心跳
                            }
                        }

                        @Override
                        public String executor() {
                            return ThreadPool.Names.SAME;
                        }
                    }
            );
        }
    }

    class PingRequestHandler implements TransportRequestHandler<PingRequest> { // NOTE: htt, 对master fault 探测的处理，这里的操作是在 非master节点上执行
        @Override
        public void messageReceived(PingRequest request, TransportChannel channel) throws Exception {
            // if we are not the node we are supposed to be pinged, send an exception
            // this can happen when a kill -9 is sent, and another node is started using the same port
            if (!localNode.equals(request.targetNode())) { // NOTE:htt, 如果当前节点不是对应的探测节点，抛出异常
                throw new IllegalStateException("Got pinged as node " + request.targetNode() + "], but I am node " + localNode );
            }

            // PingRequest will have clusterName set to null if it came from a node of version <1.4.0
            if (request.clusterName != null && !request.clusterName.equals(clusterName)) { // NOTE:htt, 如果集群不一致，也抛出异常
                // Don't introduce new exception for bwc reasons
                throw new IllegalStateException("Got pinged with cluster name [" + request.clusterName + "], but I'm part of cluster ["
                    + clusterName + "]");
            }

            notifyPingReceived(request); // NOTE: htt, 当前非master节点响应 master节点的成功的 心跳探测

            channel.sendResponse(new PingResponse()); // NOTE:htt, 回包给master节点
        }
    }


    public static class PingRequest extends TransportRequest { // NOTE: htt, 请求 非master节点 心跳请求

        // the (assumed) node we are pinging
        private DiscoveryNode targetNode; // NOTE: htt, 目标节点（master节点即将ping)

        private ClusterName clusterName;

        private DiscoveryNode masterNode; // NOTE: htt, master 节点

        private long clusterStateVersion = ClusterState.UNKNOWN_VERSION; // NOTE:htt, 当前集群版本号

        public PingRequest() {
        }

        PingRequest(DiscoveryNode targetNode, ClusterName clusterName, DiscoveryNode masterNode, long clusterStateVersion) {
            this.targetNode = targetNode;
            this.clusterName = clusterName;
            this.masterNode = masterNode;
            this.clusterStateVersion = clusterStateVersion;
        }

        public DiscoveryNode targetNode() {
            return targetNode;
        }

        public ClusterName clusterName() {
            return clusterName;
        }

        public DiscoveryNode masterNode() {
            return masterNode;
        }

        public long clusterStateVersion() {
            return clusterStateVersion;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            targetNode = new DiscoveryNode(in);
            clusterName = new ClusterName(in);
            masterNode = new DiscoveryNode(in);
            clusterStateVersion = in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            targetNode.writeTo(out);
            clusterName.writeTo(out);
            masterNode.writeTo(out);
            out.writeLong(clusterStateVersion);
        }
    }

    private static class PingResponse extends TransportResponse { // NOTE: htt, 非master节点的ping回包

        private PingResponse() {
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
        }
    }
}
