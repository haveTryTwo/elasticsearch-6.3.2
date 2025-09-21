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

import java.io.Closeable;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportConnectionListener;
import org.elasticsearch.transport.TransportService;

import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;

/**
 * A base class for {@link MasterFaultDetection} &amp; {@link NodesFaultDetection},
 * making sure both use the same setting.
 */
public abstract class FaultDetection extends AbstractComponent implements Closeable { // NOTE: htt, fault delection of ping info including interval/timeout/retryCount

    public static final Setting<Boolean> CONNECT_ON_NETWORK_DISCONNECT_SETTING =
        Setting.boolSetting("discovery.zen.fd.connect_on_network_disconnect", false, Property.NodeScope);
    public static final Setting<TimeValue> PING_INTERVAL_SETTING =
        Setting.positiveTimeSetting("discovery.zen.fd.ping_interval", timeValueSeconds(1), Property.NodeScope); // NOTE: htt, 每1s进行心跳探测
    public static final Setting<TimeValue> PING_TIMEOUT_SETTING =
        Setting.timeSetting("discovery.zen.fd.ping_timeout", timeValueSeconds(30), Property.NodeScope); // NOTE: htt, ping的超时时间为30s，非动态配置需要重启es节点
    public static final Setting<Integer> PING_RETRIES_SETTING =
        Setting.intSetting("discovery.zen.fd.ping_retries", 3, Property.NodeScope); // NOTE: htt, retry 发送3次心跳探测
    public static final Setting<Boolean> REGISTER_CONNECTION_LISTENER_SETTING =
        Setting.boolSetting("discovery.zen.fd.register_connection_listener", true, Property.NodeScope);

    protected final ThreadPool threadPool;  // NOTE: htt, 线程池
    protected final ClusterName clusterName; // NOTE: htt, 集群名字
    protected final TransportService transportService; // NOTE: htt, 建立tcp连接并发送请求到对应节点

    // used mainly for testing, should always be true
    protected final boolean registerConnectionListener;
    protected final FDConnectionListener connectionListener;
    protected final boolean connectOnNetworkDisconnect; // NOTE: htt, 连接断开时重新连接，默认为false which default is false

    protected final TimeValue pingInterval; // NOTE: htt, 每1s进行心跳探测
    protected final TimeValue pingRetryTimeout; // NOTE: htt, ping的超时时间为30s
    protected final int pingRetryCount; // NOTE: htt, retry 发送3次心跳探测，默认为3次

    public FaultDetection(Settings settings, ThreadPool threadPool, TransportService transportService, ClusterName clusterName) {
        super(settings);
        this.threadPool = threadPool;
        this.transportService = transportService;
        this.clusterName = clusterName;

        this.connectOnNetworkDisconnect = CONNECT_ON_NETWORK_DISCONNECT_SETTING.get(settings); // NOTE: htt, 连接断开时重新连接，默认为false which default is false
        this.pingInterval = PING_INTERVAL_SETTING.get(settings); // NOTE: htt, 1 second心跳探测
        this.pingRetryTimeout = PING_TIMEOUT_SETTING.get(settings); // NOTE: htt, 30s心跳超时
        this.pingRetryCount = PING_RETRIES_SETTING.get(settings);  // NOTE: htt, 心跳探测重试3次
        this.registerConnectionListener = REGISTER_CONNECTION_LISTENER_SETTING.get(settings); // NOTE: htt, register connection listener

        this.connectionListener = new FDConnectionListener();
        if (registerConnectionListener) {
            transportService.addConnectionListener(connectionListener); // NOTE:htt, 添加fd操作监听
        }
    }

    @Override
    public void close() {
        transportService.removeConnectionListener(connectionListener); // NOTE:htt, 移除fd操作监听
    }

    /**
     * This method will be called when the {@link org.elasticsearch.transport.TransportService} raised a node disconnected event
     */
    abstract void handleTransportDisconnect(DiscoveryNode node);

    private class FDConnectionListener implements TransportConnectionListener { // NOTE: htt, 监听连接断开后执行相应处理
        @Override
        public void onNodeConnected(DiscoveryNode node) {
        }

        @Override
        public void onNodeDisconnected(DiscoveryNode node) { // NOTE:htt, 当连接断开后执行相应处理
            handleTransportDisconnect(node); // NOTE: htt, hanlde transport disconnect
        }
    }

}
