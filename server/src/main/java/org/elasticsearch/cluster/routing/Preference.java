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
package org.elasticsearch.cluster.routing;


/**
 * Routing Preference Type
 */
public enum  Preference { // NOTE:htt, 查询时优先选择路由值

    /**
     * Route to specific shards
     */
    SHARDS("_shards"), // NOTE:htt, 指定_shards开头

    /**
     * Route to preferred nodes, if possible
     */
    PREFER_NODES("_prefer_nodes"), // NOTE:htt, 优先处理节点

    /**
     * Route to local node, if possible
     */
    LOCAL("_local"), // NOTE:htt, 优先local节点

    /**
     * Route to primary shards
     */
    PRIMARY("_primary"), // NOTE:htt, 路由主分片

    /**
     * Route to replica shards
     */
    REPLICA("_replica"),

    /**
     * Route to primary shards first
     */
    PRIMARY_FIRST("_primary_first"), // NOTE:htt, 优先主分片

    /**
     * Route to replica shards first
     */
    REPLICA_FIRST("_replica_first"),

    /**
     * Route to the local shard only
     */
    ONLY_LOCAL("_only_local"),

    /**
     * Route to only node with attribute
     */
    ONLY_NODES("_only_nodes");

    private final String type; // NOTE:htt, 类型

    Preference(String type) {
        this.type = type;
    }

    public String type() {
        return type;
    }
    /**
     * Parses the Preference Type given a string
     */
    public static Preference parse(String preference) { // NOTE:htt, 解析优先preference值
        String preferenceType;
        int colonIndex = preference.indexOf(':');
        if (colonIndex == -1) {
            preferenceType = preference;
        } else {
            preferenceType = preference.substring(0, colonIndex); // NOTE:htt, 获取preference的key
        }

        switch (preferenceType) { // NOTE:htt, 处理不同的preference
            case "_shards":
                return SHARDS;
            case "_prefer_nodes":
                return PREFER_NODES;
            case "_local":
                return LOCAL;
            case "_primary":
                return PRIMARY;
            case "_replica":
                return REPLICA;
            case "_primary_first":
            case "_primaryFirst":
                return PRIMARY_FIRST;
            case "_replica_first":
            case "_replicaFirst":
                return REPLICA_FIRST;
            case "_only_local":
            case "_onlyLocal":
                return ONLY_LOCAL;
            case "_only_nodes":
                return ONLY_NODES;
            default:
                throw new IllegalArgumentException("no Preference for [" + preferenceType + "]");
        }
    }

}



