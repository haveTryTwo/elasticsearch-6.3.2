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

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static org.elasticsearch.gateway.GatewayService.STATE_NOT_RECOVERED_BLOCK;

public interface ZenPing extends Releasable { // NOTE: htt, zen ping接口，包括pingCollection，其中包含一组节点和对应回包

    void start();

    void ping(Consumer<PingCollection> resultsConsumer, TimeValue timeout);

    class PingResponse implements Writeable { // NOTE: htt, ping response including clusterName/node/master/clusterStateVersion

        private static final AtomicLong idGenerator = new AtomicLong();

        // an always increasing unique identifier for this ping response.
        // lower values means older pings.
        private final long id; // NOTE:htt, 递增的id信息

        private final ClusterName clusterName; // NOTE:htt, 集群名称

        private final DiscoveryNode node; // NOTE: htt, master ping的节点

        private final DiscoveryNode master; // NOTE: htt, master节点

        private final long clusterStateVersion; // NOTE: htt, cluster state version of current node

        /**
         * @param node                the node which this ping describes
         * @param master              the current master of the node
         * @param clusterName         the cluster name of the node
         * @param clusterStateVersion the current cluster state version of that node
         *                            ({@link ElectMasterService.MasterCandidate#UNRECOVERED_CLUSTER_VERSION} for not recovered)
         */
        PingResponse(DiscoveryNode node, DiscoveryNode master, ClusterName clusterName, long clusterStateVersion) {
            this.id = idGenerator.incrementAndGet();
            this.node = node;
            this.master = master;
            this.clusterName = clusterName;
            this.clusterStateVersion = clusterStateVersion;
        }

        public PingResponse(DiscoveryNode node, DiscoveryNode master, ClusterState state) {
            this(node, master, state.getClusterName(),
                state.blocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK) ?
                    ElectMasterService.MasterCandidate.UNRECOVERED_CLUSTER_VERSION : state.version());
        }

        PingResponse(StreamInput in) throws IOException {
            this.clusterName = new ClusterName(in);
            this.node = new DiscoveryNode(in);
            this.master = in.readOptionalWriteable(DiscoveryNode::new);
            this.clusterStateVersion = in.readLong();
            this.id = in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            clusterName.writeTo(out);
            node.writeTo(out);
            out.writeOptionalWriteable(master);
            out.writeLong(clusterStateVersion);
            out.writeLong(id);
        }

        /**
         * an always increasing unique identifier for this ping response.
         * lower values means older pings.
         */
        public long id() {
            return this.id;
        }

        /**
         * the name of the cluster this node belongs to
         */
        public ClusterName clusterName() {
            return this.clusterName;
        }

        /** the node which this ping describes */
        public DiscoveryNode node() {
            return node;
        }

        /** the current master of the node */
        public DiscoveryNode master() {
            return master;
        }

        /**
         * the current cluster state version of that node ({@link ElectMasterService.MasterCandidate#UNRECOVERED_CLUSTER_VERSION}
         * for not recovered) */
        public long getClusterStateVersion() {
            return clusterStateVersion;
        }

        @Override
        public String toString() {
            return "ping_response{node [" + node + "], id[" + id + "], master [" + master + "]," +
                   "cluster_state_version [" + clusterStateVersion + "], cluster_name[" + clusterName.value() + "]}";
        }
    }


    /**
     * a utility collection of pings where only the most recent ping is stored per node
     */
    class PingCollection { // NOTE: htt, pings collection including echo ping of node

        Map<DiscoveryNode, PingResponse> pings; // NOTE:htt, 每个节点的ping的回包列表

        public PingCollection() {
            pings = new HashMap<>();
        }

        /**
         * adds a ping if newer than previous pings from the same node
         *
         * @return true if added, false o.w.
         */
        public synchronized boolean addPing(PingResponse ping) { // NOTE:htt, 添加PingResponse，需要添加的id比原有的大
            PingResponse existingResponse = pings.get(ping.node());
            // in case both existing and new ping have the same id (probably because they come
            // from nodes from version <1.4.0) we prefer to use the last added one.
            if (existingResponse == null || existingResponse.id() <= ping.id()) {
                pings.put(ping.node(), ping);
                return true;
            }
            return false;
        }

        /** serialize current pings to a list. It is guaranteed that the list contains one ping response per node */
        public synchronized List<PingResponse> toList() {
            return new ArrayList<>(pings.values());
        }

        /** the number of nodes for which there are known pings */
        public synchronized int size() {
            return pings.size();
        }
    }
}
