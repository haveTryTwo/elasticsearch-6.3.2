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

import com.carrotsearch.hppc.ObjectContainer;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class ElectMasterService extends AbstractComponent { // NOTE: htt, 候选节点个数满足要求，然后进行排序(先判断版本号(大的在前），若相等判断节点信息，id小的为在前)，再返回排序第一个候选节点作为master节点

    public static final Setting<Integer> DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING =
        Setting.intSetting("discovery.zen.minimum_master_nodes", -1, Property.Dynamic, Property.NodeScope); // NOTE:htt, 最小节点数，对于master来说

    private volatile int minimumMasterNodes; // NOTE: htt, 选主时最少多少个节点需要确认，要满足 majority 机制

    /**
     * a class to encapsulate all the information about a candidate in a master election
     * that is needed to decided which of the candidates should win
     */
    public static class MasterCandidate { // NOTE: htt, master候选节点，并提供机制从两个节点中选择哪个为主

        public static final long UNRECOVERED_CLUSTER_VERSION = -1; // NOTE: htt, unrecovered cluster version

        final DiscoveryNode node; // NOTE: htt，节点信息

        final long clusterStateVersion; // NOTE: htt, 集群版本号，用于判断选主的一个依据

        public MasterCandidate(DiscoveryNode node, long clusterStateVersion) {
            Objects.requireNonNull(node);
            assert clusterStateVersion >= -1 : "got: " + clusterStateVersion;
            assert node.isMasterNode();
            this.node = node;
            this.clusterStateVersion = clusterStateVersion;
        }

        public DiscoveryNode getNode() {
            return node;
        }

        public long getClusterStateVersion() {
            return clusterStateVersion;
        }

        @Override
        public String toString() {
            return "Candidate{" +
                "node=" + node +
                ", clusterStateVersion=" + clusterStateVersion +
                '}';
        }

        /**
         * compares two candidates to indicate which the a better master.
         * A higher cluster state version is better
         *
         * @return -1 if c1 is a batter candidate, 1 if c2.
         */
        public static int compare(MasterCandidate c1, MasterCandidate c2) { // NOTE: htt, 判断哪个候选节点为主，其中先判断版本号(大的为主），否则判断节点信息(如果是仅有一个是master节点，则选择该节点，否则选择节点id小的为master)
            // we explicitly swap c1 and c2 here. the code expects "better" is lower in a sorted
            // list, so if c2 has a higher cluster state version, it needs to come first.
            int ret = Long.compare(c2.clusterStateVersion, c1.clusterStateVersion); // NOTE: htt, 调整 c2,c1比较位置，以版本号大的值为准；compare cluster state version first, and select the bigger one
            if (ret == 0) {
                ret = compareNodes(c1.getNode(), c2.getNode()); // NOTE: htt, 如果是仅有一个是master节点，则选择该节点，否则选择节点id小的为master
            }
            return ret;
        }
    }

    public ElectMasterService(Settings settings) {
        super(settings);
        this.minimumMasterNodes = DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING.get(settings);
        logger.debug("using minimum_master_nodes [{}]", minimumMasterNodes);
    }

    public void minimumMasterNodes(int minimumMasterNodes) {
        this.minimumMasterNodes = minimumMasterNodes;
    }

    public int minimumMasterNodes() {
        return minimumMasterNodes;
    }

    public int countMasterNodes(Iterable<DiscoveryNode> nodes) { // NOTE: htt, 统计当前master角色的节点数
        int count = 0;
        for (DiscoveryNode node : nodes) {
            if (node.isMasterNode()) { // NOTE: htt, 统计是master角色
                count++;
            }
        }
        return count;
    }

    public boolean hasEnoughCandidates(Collection<MasterCandidate> candidates) { // NOTE: htt, 判断是否有足够的候选节点, number of candidate nodes >= minimum master nodes
        if (candidates.isEmpty()) {
            return false;
        }
        if (minimumMasterNodes < 1) { // NOTE: htt, 如果最小master选举个数小于1，则直接返回true
            return true;
        }
        assert candidates.stream().map(MasterCandidate::getNode).collect(Collectors.toSet()).size() == candidates.size() :
            "duplicates ahead: " + candidates;
        return candidates.size() >= minimumMasterNodes; // NOTE: htt, 判断候选节点个数是否大于 最小master选举个数
    }

    /**
     * Elects a new master out of the possible nodes, returning it. Returns <tt>null</tt>
     * if no master has been elected.
     */
    public MasterCandidate electMaster(Collection<MasterCandidate> candidates) { // NOTE: htt, 候选节点个数满足要求，然后进行排序(先判断版本号(大的在前），若相等判断节点信息，id小的为在前)，再返回排序第一个候选节点作为master节点
        assert hasEnoughCandidates(candidates); // NOTE: htt, 判断是否有足够的候选节点
        List<MasterCandidate> sortedCandidates = new ArrayList<>(candidates);
        sortedCandidates.sort(MasterCandidate::compare); // NOTE: htt, 判断哪个候选节点为主，其中先判断版本号(大的在前），否则判断节点信息(如果是仅有一个是master节点，则选择该节点，否则选择节点id小的为master)
        return sortedCandidates.get(0); // NOTE: htt, 返回第一个值
    }

    /** selects the best active master to join, where multiple are discovered */
    public DiscoveryNode tieBreakActiveMasters(Collection<DiscoveryNode> activeMasters) { // NOTE: htt, 激活的master节点中，选择最小id为master
        return activeMasters.stream().min(ElectMasterService::compareNodes).get(); // NOTE: htt, 判断节点信息(如果是仅有一个是master节点，则选择该节点，否则选择节点id小的为master)
    }

    public boolean hasEnoughMasterNodes(Iterable<DiscoveryNode> nodes) { // NOTE: htt, 判断master角色节点是否足够
        final int count = countMasterNodes(nodes);
        return count > 0 && (minimumMasterNodes < 0 || count >= minimumMasterNodes); // NOTE: htt, master角色节点是否大于 最小master选举个数
    }

    public boolean hasTooManyMasterNodes(Iterable<DiscoveryNode> nodes) { // NOTE: htt, 判断最小master选举个数 配置是否合理，如果 <= count/2，则可能选出多于2个master，即出现脑裂
        final int count = countMasterNodes(nodes);
        return count > 1 && minimumMasterNodes <= count / 2; // NOTE: htt, 判断最小master选举个数 配置是否合理，如果 <= count/2，则可能选出多于2个master，即出现脑裂
    }

    public void logMinimumMasterNodesWarningIfNecessary(ClusterState oldState, ClusterState newState) {// NOTE: htt, 新的集群状态可能出现脑裂时记录日志
        // check if min_master_nodes setting is too low and log warning
        if (hasTooManyMasterNodes(oldState.nodes()) == false && hasTooManyMasterNodes(newState.nodes())) { // NOTE: htt, 新的集群状态可能出现脑裂
            logger.warn("value for setting \"{}\" is too low. This can result in data loss! Please set it to at least a quorum of master-" +
                    "eligible nodes (current value: [{}], total number of master-eligible nodes used for publishing in this round: [{}])",
                ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING.getKey(), minimumMasterNodes(),
                newState.getNodes().getMasterNodes().size());
        }
    }

    /**
     * Returns the given nodes sorted by likelihood of being elected as master, most likely first.
     * Non-master nodes are not removed but are rather put in the end
     */
    static List<DiscoveryNode> sortByMasterLikelihood(Iterable<DiscoveryNode> nodes) { // NOTE: htt, 对节点进行排序，排序规则和master选举节点排序规则一致
        ArrayList<DiscoveryNode> sortedNodes = CollectionUtils.iterableAsArrayList(nodes);
        CollectionUtil.introSort(sortedNodes, ElectMasterService::compareNodes); // NOTE: htt, 如果是仅有一个是master节点，则选择该节点，否则选择节点id小的为master
        return sortedNodes;
    }

    /**
     * Returns a list of the next possible masters.
     */
    public DiscoveryNode[] nextPossibleMasters(ObjectContainer<DiscoveryNode> nodes, int numberOfPossibleMasters) { // NOTE: htt, 返回指定个数的可能的master节点(numberOfPossibleMasters-1个)，此处是满足相应排序
        List<DiscoveryNode> sortedNodes = sortedMasterNodes(Arrays.asList(nodes.toArray(DiscoveryNode.class)));// NOTE: htt, 保留master节点(移除非master节点),在对master节点进行排序，排序规则和master选举节点排序规则一致
        if (sortedNodes == null) {
            return new DiscoveryNode[0]; // NOTE: htt, 没有master节点，则返回空
        }
        List<DiscoveryNode> nextPossibleMasters = new ArrayList<>(numberOfPossibleMasters);
        int counter = 0;
        for (DiscoveryNode nextPossibleMaster : sortedNodes) {
            if (++counter >= numberOfPossibleMasters) {
                break;
            }
            nextPossibleMasters.add(nextPossibleMaster); // NOTE: htt, 返回指定个数的可能的master节点(numberOfPossibleMasters-1个)，此处是满足相应排序
        }
        return nextPossibleMasters.toArray(new DiscoveryNode[nextPossibleMasters.size()]);
    }

    private List<DiscoveryNode> sortedMasterNodes(Iterable<DiscoveryNode> nodes) { // NOTE: htt, 保留master节点(移除非master节点),在对master节点进行排序，排序规则和master选举节点排序规则一致
        List<DiscoveryNode> possibleNodes = CollectionUtils.iterableAsArrayList(nodes);
        if (possibleNodes.isEmpty()) {
            return null;
        }
        // clean non master nodes
        for (Iterator<DiscoveryNode> it = possibleNodes.iterator(); it.hasNext(); ) {
            DiscoveryNode node = it.next();
            if (!node.isMasterNode()) {
                it.remove(); // NOTE: htt, 移除非master节点
            }
        }
        CollectionUtil.introSort(possibleNodes, ElectMasterService::compareNodes); // NOTE: htt, 如果是仅有一个是master节点，则选择该节点，否则选择节点id小的为master
        return possibleNodes;
    }

    /** master nodes go before other nodes, with a secondary sort by id **/
     private static int compareNodes(DiscoveryNode o1, DiscoveryNode o2) { // NOTE: htt, 如果是仅有一个是master节点，则选择该节点，否则选择节点id小的为master
        if (o1.isMasterNode() && !o2.isMasterNode()) { // NOTE: htt, 如果o1为master，而o2不是，则选择o1
            return -1;
        }
        if (!o1.isMasterNode() && o2.isMasterNode()) { // NOTE: htt, 如果o2为master，而o1不是，则选择o2
            return 1;
        }
        return o1.getId().compareTo(o2.getId()); // NOTE: htt, 选择小的节点id做为主
    }
}
