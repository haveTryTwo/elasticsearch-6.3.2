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
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.Nullable;
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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A fault detection that pings the master periodically to see if its alive. NOTE: htt, data节点启动，1是定期检查master，超时时间30s
 */
public class MasterFaultDetection extends FaultDetection { // NOTE; htt, 数据节点上执行master节点的存活探测,如果成功则1s后继续探测, 否则等待30s并重试3次，如果3次依旧失败 exectue onMasterFailuer(MasterNodeFailureListener) 会重新选主（如果是断开连接则立即重新选主，并不重试）

    public static final String MASTER_PING_ACTION_NAME = "internal:discovery/zen/fd/master_ping"; // NOTE:htt, 探测master节点请求

    public interface Listener { // NOTE: htt fault detection

        /** called when pinging the master failed, like a timeout, transport disconnects etc */
        void onMasterFailure(DiscoveryNode masterNode, Throwable cause, String reason);

    }

    private final MasterService masterService; // NOTE: htt, masterService execute batch tasks and updateClusterState is has been changed
    private final java.util.function.Supplier<ClusterState> clusterStateSupplier; // NOTE:htt, 获取集群状态
    private final CopyOnWriteArrayList<Listener> listeners = new CopyOnWriteArrayList<>();

    private volatile MasterPinger masterPinger;  // NOTE: htt, 执行具体的 ping master，如果超时异常，则重试（共最多3次，每次30s超时时间），如果其他异常，则执行 notify master exception 并重新选主

    private final Object masterNodeMutex = new Object(); // NOTE:htt, master节点锁

    private volatile DiscoveryNode masterNode; // NOTE: htt, 待心跳探测的master node

    private volatile int retryCount; // NOTE: htt, current retry count

    private final AtomicBoolean notifiedMasterFailure = new AtomicBoolean(); // NOTE: htt, notify when master failure

    public MasterFaultDetection(Settings settings, ThreadPool threadPool, TransportService transportService,
                                java.util.function.Supplier<ClusterState> clusterStateSupplier, MasterService masterService,
                                ClusterName clusterName) {
        super(settings, threadPool, transportService, clusterName);
        this.clusterStateSupplier = clusterStateSupplier;
        this.masterService = masterService;

        logger.debug("[master] uses ping_interval [{}], ping_timeout [{}], ping_retries [{}]", pingInterval, pingRetryTimeout,
            pingRetryCount);

        transportService.registerRequestHandler( // NOTE: htt, 注册非master节点的探测master请求，并且注册 MasterPingRequestHandler 用于在master节点上处理 非master节点的探测请求
            MASTER_PING_ACTION_NAME, MasterPingRequest::new, ThreadPool.Names.SAME, false, false, new MasterPingRequestHandler());
    }

    public DiscoveryNode masterNode() {
        return this.masterNode;
    }

    public void addListener(Listener listener) {
        listeners.add(listener);
    }

    public void removeListener(Listener listener) {
        listeners.remove(listener);
    }

    public void restart(DiscoveryNode masterNode, String reason) { // NOTE:htt, 重新和master节点建立连接
        synchronized (masterNodeMutex) {
            if (logger.isDebugEnabled()) {
                logger.debug("[master] restarting fault detection against master [{}], reason [{}]", masterNode, reason);
            }
            innerStop();
            innerStart(masterNode);
        }
    }

    private void innerStart(final DiscoveryNode masterNode) { // NOTE:htt, 内部重启
        this.masterNode = masterNode;
        this.retryCount = 0; // NOTE: htt, init retry count to 0
        this.notifiedMasterFailure.set(false);
        if (masterPinger != null) {
            masterPinger.stop(); // NOTE: htt, old masterPinger stop
        }
        this.masterPinger = new MasterPinger(); // NOTE: htt, new master pinger, and old masterPinger will be drop without no reference

        // we start pinging slightly later to allow the chosen master to complete it's own master election
        threadPool.schedule(pingInterval, ThreadPool.Names.SAME, masterPinger); // NOTE: htt, 1 seconds 后启用心跳，探测master存活 start masterPinger
    }

    public void stop(String reason) { // NOTE:htt, 停止
        synchronized (masterNodeMutex) {
            if (masterNode != null) {
                if (logger.isDebugEnabled()) {
                    logger.debug("[master] stopping fault detection against master [{}], reason [{}]", masterNode, reason);
                }
            }
            innerStop();
        }
    }

    private void innerStop() { // NOTE:htt, 停止
        // also will stop the next ping schedule
        this.retryCount = 0; // NOTE: htt, 重试次数设置为0, retry count clear
        if (masterPinger != null) {
            masterPinger.stop(); // NOTE: htt, ping stop
            masterPinger = null;
        }
        this.masterNode = null;
    }

    @Override
    public void close() {
        super.close();
        stop("closing");
        this.listeners.clear();
    }

    @Override
    protected void handleTransportDisconnect(DiscoveryNode node) { // NOTE:htt, 节点断开后处理
        synchronized (masterNodeMutex) {
            if (!node.equals(this.masterNode)) {
                return;
            }
            if (connectOnNetworkDisconnect) { // NOTE: htt, 在断开连接时尝试连接master节点，默认为false
                try {
                    transportService.connectToNode(node); // NOTE:htt, 尝试连接节点
                    // if all is well, make sure we restart the pinger
                    if (masterPinger != null) {
                        masterPinger.stop();
                    }
                    this.masterPinger = new MasterPinger(); // NOTE:htt, 重新启动master ping
                    // we use schedule with a 0 time value to run the pinger on the pool as it will run on later
                    threadPool.schedule(TimeValue.timeValueMillis(0), ThreadPool.Names.SAME, masterPinger); // NOTE:htt, 立即发送心跳
                } catch (Exception e) {
                    logger.trace("[master] [{}] transport disconnected (with verified connect)", masterNode);
                    notifyMasterFailure(masterNode, null, "transport disconnected (with verified connect)");
                }
            } else {
                logger.trace("[master] [{}] transport disconnected", node);
                notifyMasterFailure(node, null, "transport disconnected"); // NOTE:htt, 通知master失败
            }
        }
    }

    private void notifyMasterFailure(final DiscoveryNode masterNode, final Throwable cause, final String reason) { // NOTE:htt, 集群通知master失败
        if (notifiedMasterFailure.compareAndSet(false, true)) {
            try {
                threadPool.generic().execute(() -> {
                    for (Listener listener : listeners) {
                        listener.onMasterFailure(masterNode, cause, reason); // NOTE: htt, do batch listener when master failed, 这里应该包括将 global block of no master，这样节点就不会在继续写，以及 MasterNodeFailureListener 会重新选主
                    }
                });
            } catch (EsRejectedExecutionException e) {
                logger.error("master failure notification was rejected, it's highly likely the node is shutting down", e);
            }
            stop("master failure, " + reason); // NOTE: htt, stop ping master of fault detection
        }
    }

    private class MasterPinger implements Runnable {  // NOTE: htt, 执行具体的 ping master，如果超时异常，则重试（共最多3次，每次30s超时时间），如果其他异常，则执行 notify master exception 并重新选主

        private volatile boolean running = true;

        public void stop() {
            this.running = false;
        }

        @Override
        public void run() {
            if (!running) { // NOTE:htt, 探测master节点存活，如果设置为stop则不在继续探测
                // return and don't spawn...
                return;
            }
            final DiscoveryNode masterToPing = masterNode;
            if (masterToPing == null) {
                // master is null, should not happen, but we are still running, so reschedule
                threadPool.schedule(pingInterval, ThreadPool.Names.SAME, MasterPinger.this); // NOTE:htt, 1s后重新探测
                return;
            }

            final MasterPingRequest request = new MasterPingRequest(
                clusterStateSupplier.get().nodes().getLocalNode(), masterToPing, clusterName);
            final TransportRequestOptions options = TransportRequestOptions.builder().withType(TransportRequestOptions.Type.PING)
                .withTimeout(pingRetryTimeout).build();  // NOTE: htt, 探测master节点心跳请求，并且超时时间为30s
            transportService.sendRequest(masterToPing, MASTER_PING_ACTION_NAME, request, options,
                new TransportResponseHandler<MasterPingResponseResponse>() {
                        @Override
                        public MasterPingResponseResponse newInstance() {
                            return new MasterPingResponseResponse();
                        }

                        @Override
                        public void handleResponse(MasterPingResponseResponse response) { // NOTE: htt, 心跳探测成功
                            if (!running) {
                                return;
                            }
                            // reset the counter, we got a good result
                            MasterFaultDetection.this.retryCount = 0; // NOTE:htt, 心跳探测重试设置为0，3次超时探测中，只要有1次探测成功则重置探测次数
                            // check if the master node did not get switched on us..., if it did, we simply return with no reschedule
                            if (masterToPing.equals(MasterFaultDetection.this.masterNode())) { // NOTE:htt, 对应master节点依然是master节点
                                // we don't stop on disconnection from master, we keep pinging it
                                threadPool.schedule(pingInterval, ThreadPool.Names.SAME, MasterPinger.this); // NOTE: htt, 启动下一次心跳探测，时间间隔为1s
                            }
                        }

                        @Override
                        public void handleException(TransportException exp) { // NOTE: htt, 探测master节点异常或超时
                            if (!running) {
                                return;
                            }
                            synchronized (masterNodeMutex) {
                                // check if the master node did not get switched on us...
                                if (masterToPing.equals(MasterFaultDetection.this.masterNode())) { // NOTE:htt, 如果是当前master节点
                                    if (exp instanceof ConnectTransportException || exp.getCause() instanceof ConnectTransportException) {
                                        handleTransportDisconnect(masterToPing); // NOTE: htt, 连接出错
                                        return;
                                    } else if (exp.getCause() instanceof NotMasterException) {
                                        logger.debug("[master] pinging a master {} that is no longer a master", masterNode);
                                        notifyMasterFailure(masterToPing, exp, "no longer master");
                                        return;
                                    } else if (exp.getCause() instanceof ThisIsNotTheMasterYouAreLookingForException) {
                                        logger.debug("[master] pinging a master {} that is not the master", masterNode);
                                        notifyMasterFailure(masterToPing, exp,"not master");
                                        return;
                                    } else if (exp.getCause() instanceof NodeDoesNotExistOnMasterException) {
                                        logger.debug("[master] pinging a master {} but we do not exists on it, act as if its master failure"
                                            , masterNode);
                                        notifyMasterFailure(masterToPing, exp,"do not exists on master, act as master failure");
                                        return;
                                    }
                                    // NOTE: htt, 超时情况下，就继续重试，总共最多3次，每次30s，共90s时间，然后确认master是否已经不在
                                    int retryCount = ++MasterFaultDetection.this.retryCount; // NOTE: htt, 增加重试的次数，每次超时时间30s add retry count of fault detection to ping master with 30 second timeout
                                    logger.trace(() -> new ParameterizedMessage(
                                            "[master] failed to ping [{}], retry [{}] out of [{}]",
                                            masterNode, retryCount, pingRetryCount), exp);
                                    if (retryCount >= pingRetryCount) { // NOTE: htt, 连续超过3次，每次30s，共90s，则通知master失败，
                                        logger.debug("[master] failed to ping [{}], tried [{}] times, each with maximum [{}] timeout",
                                            masterNode, pingRetryCount, pingRetryTimeout);
                                        // not good, failure
                                        notifyMasterFailure(masterToPing, null, "failed to ping, tried [" + pingRetryCount
                                            + "] times, each with  maximum [" + pingRetryTimeout + "] timeout");
                                    } else {
                                        // resend the request, not reschedule, rely on send timeout
                                        transportService.sendRequest(masterToPing, MASTER_PING_ACTION_NAME, request, options, this); // NOTE: htt, 当未到达3次，则立即继续发送心跳，retry ping master of fault detection when not arrive 3 times
                                    }
                                }
                            }
                        }

                        @Override
                        public String executor() {
                            return ThreadPool.Names.SAME; // NOTE: htt, direct thread pool
                        }
                    }
            );
        }
    }

    /** Thrown when a ping reaches the wrong node */
    static class ThisIsNotTheMasterYouAreLookingForException extends IllegalStateException { // NOTE: htt, 不应该到达的状态

        ThisIsNotTheMasterYouAreLookingForException(String msg) {
            super(msg);
        }

        ThisIsNotTheMasterYouAreLookingForException() {
        }

        @Override
        public Throwable fillInStackTrace() {
            return null;
        }
    }

    static class NodeDoesNotExistOnMasterException extends IllegalStateException { // NOTE: htt, no master exist exception
        @Override
        public Throwable fillInStackTrace() {
            return null;
        }
    }

    private class MasterPingRequestHandler implements TransportRequestHandler<MasterPingRequest> { // NOTE: htt, 处理 非master节点心跳探测，这里的操作是在 master节点上执行

        @Override
        public void messageReceived(final MasterPingRequest request, final TransportChannel channel) throws Exception {
            final DiscoveryNodes nodes = clusterStateSupplier.get().nodes();
            // check if we are really the same master as the one we seemed to be think we are
            // this can happen if the master got "kill -9" and then another node started using the same port
            if (!request.masterNode.equals(nodes.getLocalNode())) { // NOTE:htt, 如果请求master节点不是当前节点，返回错误给原有节点
                throw new ThisIsNotTheMasterYouAreLookingForException();
            }

            // ping from nodes of version < 1.4.0 will have the clustername set to null
            if (request.clusterName != null && !request.clusterName.equals(clusterName)) { // NOTE: htt, 集群名不一样，则返回出错
                logger.trace("master fault detection ping request is targeted for a different [{}] cluster then us [{}]",
                    request.clusterName, clusterName);
                throw new ThisIsNotTheMasterYouAreLookingForException("master fault detection ping request is targeted for a different ["
                    + request.clusterName + "] cluster then us [" + clusterName + "]");
            }

            // when we are elected as master or when a node joins, we use a cluster state update thread
            // to incorporate that information in the cluster state. That cluster state is published
            // before we make it available locally. This means that a master ping can come from a node
            // that has already processed the new CS but it is not known locally.
            // Therefore, if we fail we have to check again under a cluster state thread to make sure
            // all processing is finished.
            //

            if (!nodes.isLocalNodeElectedMaster() || !nodes.nodeExists(request.sourceNode)) { // NOTE: htt, 如果当前节点不是master节点,或请求节点在集群不存在
                logger.trace("checking ping from {} under a cluster state thread", request.sourceNode);
                masterService.submitStateUpdateTask("master ping (from: " + request.sourceNode + ")", new ClusterStateUpdateTask() {

                    @Override
                    public ClusterState execute(ClusterState currentState) throws Exception { // NOTE:htt, masterService先执行任务，在根据处理结果onFailure()或clusterStateProcessed(), 如果请求节点已不在集群中，则抛出异常，并发送到整个集群
                        // if we are no longer master, fail...
                        DiscoveryNodes nodes = currentState.nodes();
                        if (!nodes.nodeExists(request.sourceNode)) { // NOTE;htt, 如果当前集群状态不包含请求节点，则抛出异常处理，然后将信息发送到集群中
                            throw new NodeDoesNotExistOnMasterException();
                        }
                        return currentState;
                    }

                    @Override
                    public void onNoLongerMaster(String source) { // NOTE: htt, 当前节点如果不是master，则回异常包
                        onFailure(source, new NotMasterException("local node is not master"));
                    }

                    @Override
                    public void onFailure(String source, @Nullable Exception e) {
                        if (e == null) {
                            e = new ElasticsearchException("unknown error while processing ping");
                        }
                        try {
                            channel.sendResponse(e); // NOTE:htt, 发送当前节点不是master
                        } catch (IOException inner) {
                            inner.addSuppressed(e);
                            logger.warn("error while sending ping response", inner);
                        }
                    }

                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) { // NOTE:htt, 集群状态处理完成，则回包给请求节点
                        try {
                            channel.sendResponse(new MasterPingResponseResponse()); // NOTE:htt, 集群状态处理完毕后，回包master心跳请求
                        } catch (IOException e) {
                            logger.warn("error while sending ping response", e);
                        }
                    }
                });
            } else {
                // send a response, and note if we are connected to the master or not
                channel.sendResponse(new MasterPingResponseResponse()); // NOTE:htt, 如果当前是master节点，并且集群包含请求节点，则回心跳包成功
            }
        }
    }


    public static class MasterPingRequest extends TransportRequest { // NOTE: htt, master心跳探测请求, master ping request

        private DiscoveryNode sourceNode; // NOTE: htt, 当前节点, current node

        private DiscoveryNode masterNode; // NOTE: htt, master节点, master node
        private ClusterName clusterName; // NOTE: htt, 集群名称, cluster name

        public MasterPingRequest() {
        }

        private MasterPingRequest(DiscoveryNode sourceNode, DiscoveryNode masterNode, ClusterName clusterName) {
            this.sourceNode = sourceNode;
            this.masterNode = masterNode;
            this.clusterName = clusterName;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            sourceNode = new DiscoveryNode(in);
            masterNode = new DiscoveryNode(in);
            clusterName = new ClusterName(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            sourceNode.writeTo(out);
            masterNode.writeTo(out);
            clusterName.writeTo(out);
        }
    }

    private static class MasterPingResponseResponse extends TransportResponse { // NOTE: htt, master节点的心跳回包

        private MasterPingResponseResponse() {
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
