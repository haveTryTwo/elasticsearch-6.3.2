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

package org.elasticsearch.node;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ExponentiallyWeightedMovingAverage;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * Collects statistics about queue size, response time, and service time of
 * tasks executed on each node, making the EWMA of the values available to the
 * coordinating node.
 */
public final class ResponseCollectorService extends AbstractComponent implements ClusterStateListener { // NOTE:htt, 节点搜集回复搜集时间，包括EWMA队列长度，节点search的EWMA时间，节点的EWMA的服务时间

    private static final double ALPHA = 0.3; // NOTE:htt, 加权值为0.3

    private final ConcurrentMap<String, NodeStatistics> nodeIdToStats = ConcurrentCollections.newConcurrentMap(); // NOTE:htt, 协调节点统计目标节点的统计信息

    public ResponseCollectorService(Settings settings, ClusterService clusterService) {
        super(settings);
        clusterService.addListener(this);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.nodesRemoved()) {
            for (DiscoveryNode removedNode : event.nodesDelta().removedNodes()) {
                removeNode(removedNode.getId());
            }
        }
    }

    void removeNode(String nodeId) { // NOTE:htt, 移除节点的统计信息
        nodeIdToStats.remove(nodeId);
    }

    public void addNodeStatistics(String nodeId, int queueSize, long responseTimeNanos, long avgServiceTimeNanos) { // NOTE:htt, 添加一个节点统计数据
        nodeIdToStats.compute(nodeId, (id, ns) -> {
            if (ns == null) {
                ExponentiallyWeightedMovingAverage queueEWMA = new ExponentiallyWeightedMovingAverage(ALPHA, queueSize); // NOTE:htt, 队列EWMA值
                ExponentiallyWeightedMovingAverage responseEWMA = new ExponentiallyWeightedMovingAverage(ALPHA, responseTimeNanos); // NOTE:htt, 节点回复时间EWMA值
                return new NodeStatistics(nodeId, queueEWMA, responseEWMA, avgServiceTimeNanos); // NOTE:htt, 添加节点的统计数据
            } else {
                ns.queueSize.addValue((double) queueSize); // NOTE:htt, 添加新的队列值
                ns.responseTime.addValue((double) responseTimeNanos); // NOTE:htt, 添加回复时间
                ns.serviceTime = avgServiceTimeNanos; // NOTE:htt, 添加平均时间
                return ns;
            }
        });
    }

    public Map<String, ComputedNodeStats> getAllNodeStatistics() {
        final int clientNum = nodeIdToStats.size();
        // Transform the mutable object internally used for accounting into the computed version
        Map<String, ComputedNodeStats> nodeStats = new HashMap<>(nodeIdToStats.size());
        nodeIdToStats.forEach((k, v) -> {
            nodeStats.put(k, new ComputedNodeStats(clientNum, v));
        });
        return nodeStats;
    }

    public AdaptiveSelectionStats getAdaptiveStats(Map<String, Long> clientSearchConnections) {
        return new AdaptiveSelectionStats(clientSearchConnections, getAllNodeStatistics());
    }

    /**
     * Optionally return a {@code NodeStatistics} for the given nodeid, if
     * response information exists for the given node. Returns an empty
     * {@code Optional} if the node was not found.
     */
    public Optional<ComputedNodeStats> getNodeStatistics(final String nodeId) { // NOTE:htt, 获取目标节点统计信息
        final int clientNum = nodeIdToStats.size();
        return Optional.ofNullable(nodeIdToStats.get(nodeId)).map(ns -> new ComputedNodeStats(clientNum, ns));
    }

    /**
     * Struct-like class encapsulating a point-in-time snapshot of a particular
     * node's statistics. This includes the EWMA of queue size, response time,
     * and service time.
     */
    public static class ComputedNodeStats implements Writeable { // NOTE:htt, 计算目标节点统计数据
        // We store timestamps with nanosecond precision, however, the
        // formula specifies milliseconds, therefore we need to convert
        // the values so the times don't unduely weight the formula
        private final double FACTOR = 1000000.0;
        private final int clientNum; // NOTE:htt, 节点个数

        private double cachedRank = 0;

        public final String nodeId; // NOTE:htt, 目标节点
        public final int queueSize; // NOTE:htt, 队列大小， EWMA队列长度
        public final double responseTime; // NOTE:htt, 节点回复时间（从协调节点获取目标节点）， EWMA
        public final double serviceTime; // NOTE:htt, 节点服务时间, EWMA

        public ComputedNodeStats(String nodeId, int clientNum, int queueSize, double responseTime, double serviceTime) {
            this.nodeId = nodeId;
            this.clientNum = clientNum;
            this.queueSize = queueSize;
            this.responseTime = responseTime;
            this.serviceTime = serviceTime;
        }

        ComputedNodeStats(int clientNum, NodeStatistics nodeStats) {
            this(nodeStats.nodeId, clientNum,
                    (int) nodeStats.queueSize.getAverage(), nodeStats.responseTime.getAverage(), nodeStats.serviceTime);
        }

        ComputedNodeStats(StreamInput in) throws IOException {
            this.nodeId = in.readString();
            this.clientNum = in.readInt();
            this.queueSize = in.readInt();
            this.responseTime = in.readDouble();
            this.serviceTime = in.readDouble();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(this.nodeId);
            out.writeInt(this.clientNum);
            out.writeInt(this.queueSize);
            out.writeDouble(this.responseTime);
            out.writeDouble(this.serviceTime);
        }

        /**
         * Rank this copy of the data, according to the adaptive replica selection formula from the C3 paper
         * https://www.usenix.org/system/files/conference/nsdi15/nsdi15-paper-suresh.pdf
         */
        private double innerRank(long outstandingRequests) { // NOTE:htt, 自适应查询打分算法，核心参数是{ 目标节点未完成搜索请求个数，search延迟}，这两者越大就不会在继续发送请求
            // the concurrency compensation is defined as the number of
            // outstanding requests from the client to the node times the number
            // of clients in the system
            double concurrencyCompensation = outstandingRequests * clientNum; // NOTE:htt, outstandingRequests为目标节点上发送search未完成搜索请求个数

            // Cubic queue adjustment factor. The paper chose 3 though we could
            // potentially make this configurable if desired.
            int queueAdjustmentFactor = 3; // NOTE:htt, 3的指数

            // EWMA of queue size
            double qBar = queueSize;
            double qHatS = 1 + concurrencyCompensation + qBar; // NOTE:htt, q̂(s) = 1 + (os(s) * n) + q(s)

            // EWMA of response time
            double rS = responseTime / FACTOR; // NOTE:htt, 回复时间（从协调节点看到回复时间）
            // EWMA of service time
            double muBarS = serviceTime / FACTOR; // NOTE:htt, 服务时间（目标节点上服务的时间）

            // The final formula
            double rank = rS - (1.0 / muBarS) + (Math.pow(qHatS, queueAdjustmentFactor) / muBarS); // NOTE:htt, Ψ(s) = R(s) - 1/µ̄(s) + (q̂(s))^3 / µ̄(s)
            return rank;
        }

        public double rank(long outstandingRequests) {
            if (cachedRank == 0) {
                cachedRank = innerRank(outstandingRequests);
            }
            return cachedRank;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("ComputedNodeStats[");
            sb.append(nodeId).append("](");
            sb.append("nodes: ").append(clientNum);
            sb.append(", queue: ").append(queueSize);
            sb.append(", response time: ").append(String.format(Locale.ROOT, "%.1f", responseTime));
            sb.append(", service time: ").append(String.format(Locale.ROOT, "%.1f", serviceTime));
            sb.append(", rank: ").append(String.format(Locale.ROOT, "%.1f", rank(1)));
            sb.append(")");
            return sb.toString();
        }
    }

    /**
     * Class encapsulating a node's exponentially weighted queue size, response
     * time, and service time, however, this class is private and intended only
     * to be used for the internal accounting of {@code ResponseCollectorService}.
     */
    private static class NodeStatistics { // NOTE:htt, 节点信息统计
        final String nodeId; // NOTE:htt, 目标节点
        final ExponentiallyWeightedMovingAverage queueSize; // NOTE:htt, 队列大小 EWMA
        final ExponentiallyWeightedMovingAverage responseTime; // NOTE:htt, search回包请求延迟， EWMA
        double serviceTime; // NOTE:htt, 目标节点search 服务时间

        NodeStatistics(String nodeId,
                       ExponentiallyWeightedMovingAverage queueSizeEWMA,
                       ExponentiallyWeightedMovingAverage responseTimeEWMA,
                       double serviceTimeEWMA) {
            this.nodeId = nodeId;
            this.queueSize = queueSizeEWMA;
            this.responseTime = responseTimeEWMA;
            this.serviceTime = serviceTimeEWMA;
        }
    }
}
