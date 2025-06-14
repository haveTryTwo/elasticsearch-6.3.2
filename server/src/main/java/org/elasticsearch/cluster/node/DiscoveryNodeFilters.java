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

package org.elasticsearch.cluster.node;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

public class DiscoveryNodeFilters { // NOTE: htt, 节点过滤条件以及组合方式 （AND/OR)，判断当前是否为对应节点

    public enum OpType { // NOTE: htt, node op (and may be include, or may be require, TODO: check)
        AND,
        OR
    }

    /**
     * Validates the IP addresses in a group of {@link Settings} by looking for the keys
     * "_ip", "_host_ip", and "_publish_ip" and ensuring each of their comma separated values
     * that has no wildcards is a valid IP address.
     */
    public static final BiConsumer<String, String> IP_VALIDATOR = (propertyKey, rawValue) -> {
        if (rawValue != null) {
            if (propertyKey.endsWith("._ip") || propertyKey.endsWith("._host_ip") || propertyKey.endsWith("_publish_ip")) {
                for (String value : Strings.tokenizeToStringArray(rawValue, ",")) { // NOTE: htt, 按 , 分割ip
                    if (Regex.isSimpleMatchPattern(value) == false && InetAddresses.isInetAddress(value) == false) { // NOTE: htt, 校验ip
                        throw new IllegalArgumentException("invalid IP address [" + value + "] for [" + propertyKey + "]");
                    }
                }
            }
        }
    };

    public static DiscoveryNodeFilters buildFromKeyValue(OpType opType, Map<String, String> filters) {
        Map<String, String[]> bFilters = new HashMap<>();
        for (Map.Entry<String, String> entry : filters.entrySet()) {
            String[] values = Strings.tokenizeToStringArray(entry.getValue(), ",");
            if (values.length > 0) {
                bFilters.put(entry.getKey(), values);
            }
        }
        if (bFilters.isEmpty()) {
            return null;
        }
        return new DiscoveryNodeFilters(opType, bFilters);
    }

    private final Map<String, String[]> filters; // NOTE: htt, 一个key以及对应filters，如_ip 对应 1.1.1.1,2.2.2.2

    private final OpType opType; // NOTE: htt, 联合方式， AND 和 OR

    DiscoveryNodeFilters(OpType opType, Map<String, String[]> filters) {
        this.opType = opType;
        this.filters = filters;
    }

    private boolean matchByIP(String[] values, @Nullable String hostIp, @Nullable String publishIp) { // NOTE: htt, 遍历满足条件的ip，有满足的ip则直接返回，否则继续判断
        for (String value : values) {
            boolean matchIp = Regex.simpleMatch(value, hostIp) || Regex.simpleMatch(value, publishIp);
            if (matchIp) {
                return matchIp;
            }
        }
        return false;
    }

    public boolean match(DiscoveryNode node) {
        for (Map.Entry<String, String[]> entry : filters.entrySet()) { // NOTE: htt, and条件只要一个不符合则返回false, or条件只要一个符合返回true
            String attr = entry.getKey();
            String[] values = entry.getValue();
            if ("_ip".equals(attr)) { // NOTE: htt, _ip 判断
                // We check both the host_ip or the publish_ip
                String publishAddress = null;
                if (node.getAddress() instanceof TransportAddress) {
                    publishAddress = NetworkAddress.format(node.getAddress().address().getAddress());
                }
                // NOTE: htt, 判断ip是否相等
                boolean match = matchByIP(values, node.getHostAddress(), publishAddress);

                if (opType == OpType.AND) { // NOTE: htt, and would check all ip
                    if (match) {
                        // If we match, we can check to the next filter
                        continue;
                    }
                    return false;
                }

                if (match && opType == OpType.OR) { // NOTE: htt, OR组合下，只要有一个 attribute 满足即返回yes
                    return true;
                }
            } else if ("_host_ip".equals(attr)) {
                // We check explicitly only the host_ip
                boolean match = matchByIP(values, node.getHostAddress(), null);
                if (opType == OpType.AND) {
                    if (match) {
                        // If we match, we can check to the next filter
                        continue;
                    }
                    return false;
                }

                if (match && opType == OpType.OR) { // NOTE: htt, OR组合下，只要有一个 attribute 满足即返回yes
                    return true;
                }
            } else if ("_publish_ip".equals(attr)) {
                // We check explicitly only the publish_ip
                String address = null;
                if (node.getAddress() instanceof TransportAddress) {
                    address = NetworkAddress.format(node.getAddress().address().getAddress());
                }

                boolean match = matchByIP(values, address, null);
                if (opType == OpType.AND) {
                    if (match) {
                        // If we match, we can check to the next filter
                        continue;
                    }
                    return false;
                }

                if (match && opType == OpType.OR) {
                    return true;
                }
            } else if ("_host".equals(attr)) {
                for (String value : values) {
                    if (Regex.simpleMatch(value, node.getHostName())) {
                        if (opType == OpType.OR) {
                            return true;
                        }
                    } else {
                        if (opType == OpType.AND) {
                            return false;
                        }
                    }
                    if (Regex.simpleMatch(value, node.getHostAddress())) {
                        if (opType == OpType.OR) {
                            return true;
                        }
                    } else {
                        if (opType == OpType.AND) {
                            return false;
                        }
                    }
                }
            } else if ("_id".equals(attr)) {
                for (String value : values) {
                    if (node.getId().equals(value)) {
                        if (opType == OpType.OR) {
                            return true;
                        }
                    } else {
                        if (opType == OpType.AND) {
                            return false;
                        }
                    }
                }
            } else if ("_name".equals(attr) || "name".equals(attr)) {
                for (String value : values) {
                    if (Regex.simpleMatch(value, node.getName())) {
                        if (opType == OpType.OR) {
                            return true;
                        }
                    } else {
                        if (opType == OpType.AND) {
                            return false;
                        }
                    }
                }
            } else {
                String nodeAttributeValue = node.getAttributes().get(attr);
                if (nodeAttributeValue == null) {
                    if (opType == OpType.AND) {
                        return false;
                    } else {
                        continue;
                    }
                }
                for (String value : values) {
                    if (Regex.simpleMatch(value, nodeAttributeValue)) {
                        if (opType == OpType.OR) {
                            return true;
                        }
                    } else {
                        if (opType == OpType.AND) {
                            return false;
                        }
                    }
                }
            }
        }
        if (opType == OpType.OR) {
            return false;
        } else {
            return true;
        }
    }

    /**
     * Generates a human-readable string for the DiscoverNodeFilters.
     * Example: {@code _id:"id1 OR blah",name:"blah OR name2"}
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        int entryCount = filters.size();
        for (Map.Entry<String, String[]> entry : filters.entrySet()) {
            String attr = entry.getKey();
            String[] values = entry.getValue();
            sb.append(attr);
            sb.append(":\"");
            int valueCount = values.length;
            for (String value : values) {
                sb.append(value);
                if (valueCount > 1) {
                    sb.append(" ").append(opType.toString()).append(" ");
                }
                valueCount--;
            }
            sb.append("\"");
            if (entryCount > 1) {
                sb.append(",");
            }
            entryCount--;
        }
        return sb.toString();
    }
}
