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

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.EmptyTransportResponseHandler;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

public class MembershipAction extends AbstractComponent { // NOTE:htt, 处理节点加入、验证、离开请求

    public static final String DISCOVERY_JOIN_ACTION_NAME = "internal:discovery/zen/join";
    public static final String DISCOVERY_JOIN_VALIDATE_ACTION_NAME = "internal:discovery/zen/join/validate";
    public static final String DISCOVERY_LEAVE_ACTION_NAME = "internal:discovery/zen/leave";

    public interface JoinCallback { // NOTE:htt, 加入callback
        void onSuccess();

        void onFailure(Exception e);
    }

    public interface MembershipListener { // NOTE:htt, 成员加入或离开监听
        void onJoin(DiscoveryNode node, JoinCallback callback);

        void onLeave(DiscoveryNode node);
    }

    private final TransportService transportService; // NOTE: htt, 建立tcp连接并发送请求到对应节点

    private final MembershipListener listener; // NOTE:htt, 成员加入或离开监听

    public MembershipAction(Settings settings, TransportService transportService, MembershipListener listener,
                            Collection<BiConsumer<DiscoveryNode,ClusterState>> joinValidators) {
        super(settings);
        this.transportService = transportService;
        this.listener = listener;


        transportService.registerRequestHandler(DISCOVERY_JOIN_ACTION_NAME, JoinRequest::new,
            ThreadPool.Names.GENERIC, new JoinRequestRequestHandler());
        transportService.registerRequestHandler(DISCOVERY_JOIN_VALIDATE_ACTION_NAME,
            () -> new ValidateJoinRequest(), ThreadPool.Names.GENERIC,
            new ValidateJoinRequestRequestHandler(transportService::getLocalNode, joinValidators));
        transportService.registerRequestHandler(DISCOVERY_LEAVE_ACTION_NAME, LeaveRequest::new,
            ThreadPool.Names.GENERIC, new LeaveRequestRequestHandler());
    }

    public void sendLeaveRequest(DiscoveryNode masterNode, DiscoveryNode node) { // NOTE:htt, master节点发送离开请求给node
        transportService.sendRequest(node, DISCOVERY_LEAVE_ACTION_NAME, new LeaveRequest(masterNode),
            EmptyTransportResponseHandler.INSTANCE_SAME);
    }

    public void sendLeaveRequestBlocking(DiscoveryNode masterNode, DiscoveryNode node, TimeValue timeout) { // NOTE:htt, node节点发送离开请求给mater节点，并在超时时间范围内等待
        transportService.submitRequest(masterNode, DISCOVERY_LEAVE_ACTION_NAME, new LeaveRequest(node),
            EmptyTransportResponseHandler.INSTANCE_SAME).txGet(timeout.millis(), TimeUnit.MILLISECONDS);
    }

    public void sendJoinRequestBlocking(DiscoveryNode masterNode, DiscoveryNode node, TimeValue timeout) { // NOTE:htt, node节点发送加入请求给mater节点，并在超时时间范围内等待
        transportService.submitRequest(masterNode, DISCOVERY_JOIN_ACTION_NAME, new JoinRequest(node),
            EmptyTransportResponseHandler.INSTANCE_SAME).txGet(timeout.millis(), TimeUnit.MILLISECONDS);
    }

    /**
     * Validates the join request, throwing a failure if it failed.
     */
    public void sendValidateJoinRequestBlocking(DiscoveryNode node, ClusterState state, TimeValue timeout) { // NOTE:htt, 发送验证请求给node节点，并在超时时间范围内等待
        transportService.submitRequest(node, DISCOVERY_JOIN_VALIDATE_ACTION_NAME, new ValidateJoinRequest(state),
            EmptyTransportResponseHandler.INSTANCE_SAME).txGet(timeout.millis(), TimeUnit.MILLISECONDS);
    }

    public static class JoinRequest extends TransportRequest { // NOTE: htt, join request including node

        DiscoveryNode node; //  NOTE: htt, local node which would try to jion master

        public JoinRequest() {
        }

        private JoinRequest(DiscoveryNode node) {
            this.node = node;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            node = new DiscoveryNode(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            node.writeTo(out);
        }
    }


    private class JoinRequestRequestHandler implements TransportRequestHandler<JoinRequest> { // NOTE:htt, 处理节点加入请求

        @Override
        public void messageReceived(final JoinRequest request, final TransportChannel channel) throws Exception {
            listener.onJoin(request.node, new JoinCallback() {
                @Override
                public void onSuccess() {
                    try {
                        channel.sendResponse(TransportResponse.Empty.INSTANCE); // NOTE:htt, 加入成功则回包
                    } catch (Exception e) {
                        onFailure(e);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    try {
                        channel.sendResponse(e); // NOTE:htt, 否则会异常包
                    } catch (Exception inner) {
                        inner.addSuppressed(e);
                        logger.warn("failed to send back failure on join request", inner);
                    }
                }
            });
        }
    }

    static class ValidateJoinRequest extends TransportRequest { // NOTE:htt, 验证加入请求
        private ClusterState state; // NOTE:htt, 集群状态

        ValidateJoinRequest() {}

        ValidateJoinRequest(ClusterState state) {
            this.state = state;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            this.state = ClusterState.readFrom(in, null);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            this.state.writeTo(out);
        }
    }

    static class ValidateJoinRequestRequestHandler implements TransportRequestHandler<ValidateJoinRequest> { // NOTE:htt, 验证节点加入请求
        private final Supplier<DiscoveryNode> localNodeSupplier; // NOTE:htt, 本地节点
        private final Collection<BiConsumer<DiscoveryNode, ClusterState>> joinValidators;

        ValidateJoinRequestRequestHandler(Supplier<DiscoveryNode> localNodeSupplier,
                                          Collection<BiConsumer<DiscoveryNode, ClusterState>> joinValidators) {
            this.localNodeSupplier = localNodeSupplier;
            this.joinValidators = joinValidators;
        }

        @Override
        public void messageReceived(ValidateJoinRequest request, TransportChannel channel) throws Exception { // NOTE:htt, 验证节点加入
            DiscoveryNode node = localNodeSupplier.get();
            assert node != null : "local node is null";
            joinValidators.stream().forEach(action -> action.accept(node, request.state)); // NOTE:htt, 验证节点以及对应的集群状态
            channel.sendResponse(TransportResponse.Empty.INSTANCE); // NOTE:htt, 成功后正常回包
        }
    }

    /**
     * Ensures that all indices are compatible with the given node version. This will ensure that all indices in the given metadata
     * will not be created with a newer version of elasticsearch as well as that all indices are newer or equal to the minimum index
     * compatibility version.
     * @see Version#minimumIndexCompatibilityVersion()
     * @throws IllegalStateException if any index is incompatible with the given version
     */
    static void ensureIndexCompatibility(final Version nodeVersion, MetaData metaData) {
        Version supportedIndexVersion = nodeVersion.minimumIndexCompatibilityVersion();
        // we ensure that all indices in the cluster we join are compatible with us no matter if they are
        // closed or not we can't read mappings of these indices so we need to reject the join...
        for (IndexMetaData idxMetaData : metaData) {
            if (idxMetaData.getCreationVersion().after(nodeVersion)) {
                throw new IllegalStateException("index " + idxMetaData.getIndex() + " version not supported: "
                    + idxMetaData.getCreationVersion() + " the node version is: " + nodeVersion);
            }
            if (idxMetaData.getCreationVersion().before(supportedIndexVersion)) {
                throw new IllegalStateException("index " + idxMetaData.getIndex() + " version not supported: "
                    + idxMetaData.getCreationVersion() + " minimum compatible index version is: " + supportedIndexVersion);
            }
        }
    }

    /** ensures that the joining node has a version that's compatible with all current nodes*/
    static void ensureNodesCompatibility(final Version joiningNodeVersion, DiscoveryNodes currentNodes) {
        final Version minNodeVersion = currentNodes.getMinNodeVersion();
        final Version maxNodeVersion = currentNodes.getMaxNodeVersion();
        ensureNodesCompatibility(joiningNodeVersion, minNodeVersion, maxNodeVersion);
    }

    /** ensures that the joining node has a version that's compatible with a given version range */
    static void ensureNodesCompatibility(Version joiningNodeVersion, Version minClusterNodeVersion, Version maxClusterNodeVersion) {
        assert minClusterNodeVersion.onOrBefore(maxClusterNodeVersion) : minClusterNodeVersion + " > " + maxClusterNodeVersion;
        if (joiningNodeVersion.isCompatible(maxClusterNodeVersion) == false) {
            throw new IllegalStateException("node version [" + joiningNodeVersion + "] is not supported. " +
                "The cluster contains nodes with version [" + maxClusterNodeVersion + "], which is incompatible.");
        }
        if (joiningNodeVersion.isCompatible(minClusterNodeVersion) == false) {
            throw new IllegalStateException("node version [" + joiningNodeVersion + "] is not supported." +
                "The cluster contains nodes with version [" + minClusterNodeVersion + "], which is incompatible.");
        }
    }

    /**
     * ensures that the joining node's major version is equal or higher to the minClusterNodeVersion. This is needed
     * to ensure that if the master is already fully operating under the new major version, it doesn't go back to mixed
     * version mode
     **/
    static void ensureMajorVersionBarrier(Version joiningNodeVersion, Version minClusterNodeVersion) {
        final byte clusterMajor = minClusterNodeVersion.major;
        if (joiningNodeVersion.major < clusterMajor) {
            throw new IllegalStateException("node version [" + joiningNodeVersion + "] is not supported. " +
                "All nodes in the cluster are of a higher major [" + clusterMajor + "].");
        }
    }

    public static class LeaveRequest extends TransportRequest { // NOTE:htt, 节点离开请求

        private DiscoveryNode node; // NOTE:htt, 离开节点

        public LeaveRequest() {
        }

        private LeaveRequest(DiscoveryNode node) {
            this.node = node;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            node = new DiscoveryNode(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            node.writeTo(out);
        }
    }

    private class LeaveRequestRequestHandler implements TransportRequestHandler<LeaveRequest> { // NOTE:htt, 节点离开请求处理

        @Override
        public void messageReceived(LeaveRequest request, TransportChannel channel) throws Exception {
            listener.onLeave(request.node); // NOTE:htt, 处理节点离开请求
            channel.sendResponse(TransportResponse.Empty.INSTANCE); // NOTE:htt, 处理成功后回包
        }
    }
}
