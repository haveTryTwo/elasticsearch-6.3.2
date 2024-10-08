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

import org.elasticsearch.cluster.node.DiscoveryNode;

import java.util.List;

/**
 * A pluggable provider of the list of unicast hosts to use for unicast discovery.
 */
public interface UnicastHostsProvider { // NOTE: htt, unicast hosts to use for unicast discovery

    /**
     * Builds the dynamic list of unicast hosts to be used for unicast discovery.
     */
    List<DiscoveryNode> buildDynamicNodes(); // NOTE:htt, 绑定 动态节点
}
