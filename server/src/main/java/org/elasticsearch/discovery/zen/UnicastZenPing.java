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

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor;
import org.elasticsearch.common.util.concurrent.KeyedLock;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.ConnectionProfile;
import org.elasticsearch.transport.NodeNotConnectedException;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.Transport.Connection;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentMap;

public class UnicastZenPing extends AbstractComponent implements ZenPing { // NOTE:htt, 实现ZenPing接口

    public static final String ACTION_NAME = "internal:discovery/zen/unicast"; // NOTE:htt, zeng ping请求
    public static final Setting<List<String>> DISCOVERY_ZEN_PING_UNICAST_HOSTS_SETTING =
        Setting.listSetting("discovery.zen.ping.unicast.hosts", emptyList(), Function.identity(),
            Property.NodeScope);
    public static final Setting<Integer> DISCOVERY_ZEN_PING_UNICAST_CONCURRENT_CONNECTS_SETTING =
        Setting.intSetting("discovery.zen.ping.unicast.concurrent_connects", 10, 0, Property.NodeScope); // NOTE:htt, 并行运行线程数
    public static final Setting<TimeValue> DISCOVERY_ZEN_PING_UNICAST_HOSTS_RESOLVE_TIMEOUT =
        Setting.positiveTimeSetting("discovery.zen.ping.unicast.hosts.resolve_timeout", TimeValue.timeValueSeconds(5), Property.NodeScope); // NOTE:htt, 解析超时时间，默认是5s

    // these limits are per-address
    public static final int LIMIT_FOREIGN_PORTS_COUNT = 1;
    public static final int LIMIT_LOCAL_PORTS_COUNT = 5;

    private final ThreadPool threadPool; // NOTE: htt, thread pool keeping all type(index/search...) and its thread pool
    private final TransportService transportService; // NOTE: htt, 建立tcp连接并发送请求到对应节点, transportSevice to open connection to DiscoveryNode and sendRequest to node
    private final ClusterName clusterName; // NOTE:htt, 集群名称

    private final List<String> configuredHosts; // NOTE: htt, 配置的节点列表

    private final int limitPortCounts; // NOTE: htt, 限制的端口数量

    private final PingContextProvider contextProvider; // NOTE: htt, provide current cluster state

    private final AtomicInteger pingingRoundIdGenerator = new AtomicInteger(); // NOTE:htt, 生成pingingRoundId

    // used as a node id prefix for configured unicast host nodes/address
    private static final String UNICAST_NODE_PREFIX = "#zen_unicast_"; // NOTE: htt, 节点前缀

    private final Map<Integer, PingingRound> activePingingRounds = newConcurrentMap(); // NOTE:htt, 活跃的pingingRound列表

    // a list of temporal responses a node will return for a request (holds responses from other nodes)
    private final Queue<PingResponse> temporalResponses = ConcurrentCollections.newQueue(); // NOTE:htt, 临时回包请求处理

    private final UnicastHostsProvider hostsProvider; // NOTE:htt, 主机提供者

    protected final EsThreadPoolExecutor unicastZenPingExecutorService; // NOTE:htt, 执行器服务

    private final TimeValue resolveTimeout; // NOTE:htt, 解析超时时间，默认是5s

    private volatile boolean closed = false; // NOTE:htt, 是否关闭

    public UnicastZenPing(Settings settings, ThreadPool threadPool, TransportService transportService,
                          UnicastHostsProvider unicastHostsProvider, PingContextProvider contextProvider) {
        super(settings);
        this.threadPool = threadPool;
        this.transportService = transportService;
        this.clusterName = ClusterName.CLUSTER_NAME_SETTING.get(settings);
        this.hostsProvider = unicastHostsProvider;
        this.contextProvider = contextProvider;

        final int concurrentConnects = DISCOVERY_ZEN_PING_UNICAST_CONCURRENT_CONNECTS_SETTING.get(settings);
        if (DISCOVERY_ZEN_PING_UNICAST_HOSTS_SETTING.exists(settings)) {
            configuredHosts = DISCOVERY_ZEN_PING_UNICAST_HOSTS_SETTING.get(settings); // NOTE:htt, 获取配置的节点列表
            // we only limit to 1 addresses, makes no sense to ping 100 ports
            limitPortCounts = LIMIT_FOREIGN_PORTS_COUNT; // NOTE:htt, 限制的端口数量
        } else { // NOTE:htt, 如果配置的节点列表不存在，则获取本地机器的地址
            // if unicast hosts are not specified, fill with simple defaults on the local machine
            configuredHosts = transportService.getLocalAddresses(); // NOTE:htt, 获取本地机器的地址
            limitPortCounts = LIMIT_LOCAL_PORTS_COUNT; // NOTE:htt, 限制的端口数量
        }
        resolveTimeout = DISCOVERY_ZEN_PING_UNICAST_HOSTS_RESOLVE_TIMEOUT.get(settings); // NOTE:htt, 获取解析超时时间，默认是5s
        logger.debug(
            "using initial hosts {}, with concurrent_connects [{}], resolve_timeout [{}]",
            configuredHosts,
            concurrentConnects,
            resolveTimeout);

        transportService.registerRequestHandler(ACTION_NAME, ThreadPool.Names.SAME, UnicastPingRequest::new,
            new UnicastPingRequestHandler()); // NOTE:htt, 注册请求处理器

        final ThreadFactory threadFactory = EsExecutors.daemonThreadFactory(settings, "[unicast_connect]"); // NOTE:htt, 创建线程工厂
        unicastZenPingExecutorService = EsExecutors.newScaling( // NOTE:htt, 创建执行器服务
                nodeName() + "/" + "unicast_connect", // NOTE:htt, 线程名称
                0,
                concurrentConnects, // NOTE:htt, 并行运行线程数
                60, // NOTE:htt, 线程池大小
                TimeUnit.SECONDS, // NOTE:htt, 时间单位
                threadFactory, // NOTE:htt, 线程工厂
                threadPool.getThreadContext()); // NOTE:htt, 线程上下文
    }

    /**
     * Resolves a list of hosts to a list of discovery nodes. Each host is resolved into a transport address (or a collection of addresses
     * if the number of ports is greater than one) and the transport addresses are used to created discovery nodes. Host lookups are done
     * in parallel using specified executor service up to the specified resolve timeout.
     *
     * @param executorService  the executor service used to parallelize hostname lookups
     * @param logger           logger used for logging messages regarding hostname lookups
     * @param hosts            the hosts to resolve
     * @param limitPortCounts  the number of ports to resolve (should be 1 for non-local transport)
     * @param transportService the transport service
     * @param nodeId_prefix    a prefix to use for node ids
     * @param resolveTimeout   the timeout before returning from hostname lookups
     * @return a list of discovery nodes with resolved transport addresses
     */
    public static List<DiscoveryNode> resolveHostsLists(
        final ExecutorService executorService,
        final Logger logger,
        final List<String> hosts,
        final int limitPortCounts,
        final TransportService transportService,
        final String nodeId_prefix,
        final TimeValue resolveTimeout) throws InterruptedException { // NOTE:htt, 解析hosts列表, 返回discoveryNodes列表
        Objects.requireNonNull(executorService);
        Objects.requireNonNull(logger);
        Objects.requireNonNull(hosts);
        Objects.requireNonNull(transportService); // NOTE:htt, 检测不为空
        Objects.requireNonNull(nodeId_prefix);
        Objects.requireNonNull(resolveTimeout); // NOTE:htt, 检测不为空
        if (resolveTimeout.nanos() < 0) { // NOTE:htt, 如果resolveTimeout小于0则抛出异常
            throw new IllegalArgumentException("resolve timeout must be non-negative but was [" + resolveTimeout + "]");
        }
        // create tasks to submit to the executor service; we will wait up to resolveTimeout for these tasks to complete
        // 创建任务提交到executorService; 我们将在resolveTimeout时间内等待这些任务完成
        final List<Callable<TransportAddress[]>> callables =
            hosts
                .stream()
                .map(hn -> (Callable<TransportAddress[]>) () -> transportService.addressesFromString(hn, limitPortCounts))
                .collect(Collectors.toList()); // NOTE:htt, 将hosts列表转换为Callable列表
        final List<Future<TransportAddress[]>> futures =
            executorService.invokeAll(callables, resolveTimeout.nanos(), TimeUnit.NANOSECONDS); // NOTE:htt, 调用executorService的invokeAll方法
        final List<DiscoveryNode> discoveryNodes = new ArrayList<>(); // NOTE:htt, 创建discoveryNodes列表
        final Set<TransportAddress> localAddresses = new HashSet<>(); // NOTE:htt, 创建localAddresses列表
        localAddresses.add(transportService.boundAddress().publishAddress());  // NOTE:htt, 添加publishAddress到localAddresses列表
        localAddresses.addAll(Arrays.asList(transportService.boundAddress().boundAddresses())); // NOTE:htt, 添加boundAddresses到localAddresses列表
        // ExecutorService#invokeAll guarantees that the futures are returned in the iteration order of the tasks so we can associate the
        // hostname with the corresponding task by iterating together
        // ExecutorService#invokeAll 保证返回的future列表与callables列表的顺序一致，所以我们可以关联hostname预对应任务
        final Iterator<String> it = hosts.iterator();  // NOTE:htt, 创建it迭代器
        for (final Future<TransportAddress[]> future : futures) { // NOTE:htt, 遍历future列表
            final String hostname = it.next(); // NOTE:htt, 获取hostname，其中hosts和future列表是顺序对应的
            if (!future.isCancelled()) { // NOTE:htt, 如果future没有被取消
                assert future.isDone(); // NOTE:htt, 则future必定完成
                try {
                    final TransportAddress[] addresses = future.get(); // NOTE:htt, 获取addresses
                    logger.trace("resolved host [{}] to {}", hostname, addresses); // NOTE:htt, 打印resolved host [{}] to [{}]
                    for (int addressId = 0; addressId < addresses.length; addressId++) { // NOTE:htt, 遍历addresses列表
                        final TransportAddress address = addresses[addressId]; // NOTE:htt, 获取address
                        // no point in pinging ourselves
                        if (localAddresses.contains(address) == false) { // NOTE:htt, 如果address不在localAddresses列表中
                            discoveryNodes.add( // NOTE:htt, 添加discoveryNode到discoveryNodes列表
                                new DiscoveryNode(
                                    nodeId_prefix + hostname + "_" + addressId + "#",
                                    address,
                                    emptyMap(),
                                    emptySet(),
                                    Version.CURRENT.minimumCompatibilityVersion()));
                        }
                    }
                } catch (final ExecutionException e) {
                    assert e.getCause() != null;
                    final String message = "failed to resolve host [" + hostname + "]";
                    logger.warn(message, e.getCause());
                }
            } else {
                logger.warn("timed out after [{}] resolving host [{}]", resolveTimeout, hostname);
            }
        }
        return discoveryNodes; // NOTE:htt, 返回discoveryNodes列表
    }

    @Override
    public void close() { // NOTE:htt, 关闭ping线程
        ThreadPool.terminate(unicastZenPingExecutorService, 10, TimeUnit.SECONDS); // NOTE:htt, 终止ping线程服务
        Releasables.close(activePingingRounds.values());
        closed = true; // NOTE:htt, 设置closed为true
    }

    @Override
    public void start() { // NOTE:htt, 启动ping线程
    }

    /**
     * Clears the list of cached ping responses.
     * NOTE:htt, 清空临时响应列表
     */
    public void clearTemporalResponses() { // NOTE:htt, 清空临时响应列表
        temporalResponses.clear(); // NOTE:htt, 清空临时响应列表
    }

    /**
     * Sends three rounds of pings notifying the specified {@link Consumer} when pinging is complete. Pings are sent after resolving
     * configured unicast hosts to their IP address (subject to DNS caching within the JVM). A batch of pings is sent, then another batch
     * of pings is sent at half the specified {@link TimeValue}, and then another batch of pings is sent at the specified {@link TimeValue}.
     * The pings that are sent carry a timeout of 1.25 times the specified {@link TimeValue}. When pinging each node, a connection and
     * handshake is performed, with a connection timeout of the specified {@link TimeValue}.
     *
     * @param resultsConsumer the callback when pinging is complete
     * @param duration        the timeout for various components of the pings
     */
    @Override
    public void ping(final Consumer<PingCollection> resultsConsumer, final TimeValue duration) { // NOTE:htt, ping请求，超时时间默认是3s, resultsConsumer搜集ping回包结果
        ping(resultsConsumer, duration, duration);
    }

    /**
     * a variant of {@link #ping(Consumer, TimeValue)}, but allows separating the scheduling duration
     * from the duration used for request level time outs. This is useful for testing
     */
    protected void ping(final Consumer<PingCollection> resultsConsumer,
                        final TimeValue scheduleDuration,
                        final TimeValue requestDuration) { // NOTE:htt, ping请求，超时时间默认是3s, resultsConsumer搜集ping回包结果
        final List<DiscoveryNode> seedNodes;
        try {
            seedNodes = resolveHostsLists(
                unicastZenPingExecutorService,
                logger,
                configuredHosts,
                limitPortCounts,
                transportService,
                UNICAST_NODE_PREFIX,
                resolveTimeout); // NOTE:htt, 解析hosts列表，返回discoveryNodes列表
        } catch (InterruptedException e) {
            throw new RuntimeException(e); // NOTE:htt, 抛出异常
        }
        seedNodes.addAll(hostsProvider.buildDynamicNodes()); // NOTE:htt, 合并动态节点
        final DiscoveryNodes nodes = contextProvider.clusterState().nodes();
        // add all possible master nodes that were active in the last known cluster configuration
        // NOTE:htt, 合并其他节点获取到master节点
        for (ObjectCursor<DiscoveryNode> masterNode : nodes.getMasterNodes().values()) {
            seedNodes.add(masterNode.value); // NOTE: htt, 合并其他节点获取到master节点
        }

        final ConnectionProfile connectionProfile =
            ConnectionProfile.buildSingleChannelProfile(TransportRequestOptions.Type.REG, requestDuration, requestDuration); // NOTE: htt, join cluster, titmeout默认 is 3s，请求类型的个数的连接profile
        final PingingRound pingingRound = new PingingRound(pingingRoundIdGenerator.incrementAndGet(), seedNodes, resultsConsumer,
            nodes.getLocalNode(), connectionProfile);
        activePingingRounds.put(pingingRound.id(), pingingRound); // NOTE: htt, 将pingingRound添加到activePingingRounds
        final AbstractRunnable pingSender = new AbstractRunnable() { // NOTE:htt, ping线程
            @Override
            public void onFailure(Exception e) {
                if (e instanceof AlreadyClosedException == false) {
                    logger.warn("unexpected error while pinging", e);
                }
            }

            @Override
            protected void doRun() throws Exception {
                sendPings(requestDuration, pingingRound); // NOTE:htt, 发送获取列表请求，超时时间默认是3s
            }
        };
        threadPool.generic().execute(pingSender); // NOTE: htt, ping first time immediately, ping second time after 1s, ping third time after 2s
        threadPool.schedule(TimeValue.timeValueMillis(scheduleDuration.millis() / 3), ThreadPool.Names.GENERIC, pingSender); // NOTE: htt, ping second time after 1s
        threadPool.schedule(TimeValue.timeValueMillis(scheduleDuration.millis() / 3 * 2), ThreadPool.Names.GENERIC, pingSender); // NOTE: htt, ping third time after 2s
        threadPool.schedule(scheduleDuration, ThreadPool.Names.GENERIC, new AbstractRunnable() { // NOTE: htt, execute after 3s，执行关闭
            @Override
            protected void doRun() throws Exception {
                finishPingingRound(pingingRound); // NOTE:htt, 完成ping线程，即关闭pingingRound
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn("unexpected error while finishing pinging round", e);
            }
        });
    }

    // for testing
    protected void finishPingingRound(PingingRound pingingRound) { // NOTE:htt, 完成ping线程
        pingingRound.close(); // NOTE:htt, 关闭pingingRound
    }

    protected class PingingRound implements Releasable { // NOTE:htt, pingingRound
        private final int id; // NOTE:htt, id
        private final Map<TransportAddress, Connection> tempConnections = new HashMap<>(); // NOTE:htt, 临时连接，对应目标 ip,port 的链接
        private final KeyedLock<TransportAddress> connectionLock = new KeyedLock<>(true); // NOTE:htt, 连接锁
        private final PingCollection pingCollection; // NOTE:htt, ping集合，包含每个节点的ping回包
        private final List<DiscoveryNode> seedNodes; // NOTE:htt, 种子节点
        private final Consumer<PingCollection> pingListener; // NOTE:htt, ping监听器
        private final DiscoveryNode localNode; // NOTE:htt, local节点
        private final ConnectionProfile connectionProfile; // NOTE:htt, 连接配置

        private AtomicBoolean closed = new AtomicBoolean(false); // NOTE:htt, 是否已关闭

        PingingRound(int id, List<DiscoveryNode> seedNodes, Consumer<PingCollection> resultsConsumer, DiscoveryNode localNode,
                     ConnectionProfile connectionProfile) {
            this.id = id;
            this.seedNodes = Collections.unmodifiableList(new ArrayList<>(seedNodes));
            this.pingListener = resultsConsumer;
            this.localNode = localNode;
            this.connectionProfile = connectionProfile;
            this.pingCollection = new PingCollection();
        }

        public int id() {
            return this.id;
        }

        public boolean isClosed() {
            return this.closed.get();
        }

        public List<DiscoveryNode> getSeedNodes() { // NOTE:htt, 获取种子节点
            ensureOpen(); // NOTE:htt, 确保pingingRound是打开的
            return seedNodes;
        }

        public Connection getOrConnect(DiscoveryNode node) throws IOException { // NOTE:htt, 获取或连接节点
            Connection result;
            try (Releasable ignore = connectionLock.acquire(node.getAddress())) { // NOTE:htt, 针对node.getAddress()（即ip和port）获取锁
                result = tempConnections.get(node.getAddress()); // NOTE:htt, 从临时链接中获取节点的连接
                if (result == null) { // NOTE:htt, 如果连接不存在
                    ensureOpen();
                    boolean success = false;
                    logger.trace("[{}] opening connection to [{}]", id(), node);
                    result = transportService.openConnection(node, connectionProfile); // NOTE:htt, 打开连接，建立和node节点的连接
                    try {
                        transportService.handshake(result, connectionProfile.getHandshakeTimeout().millis()); // NOTE:htt, 握手，握手验证连接，默认超时时间3s
                        synchronized (this) {
                            // acquire lock and check if closed, to prevent leaving an open connection after closing
                            ensureOpen(); // NOTE:htt, 确保pingingRound是打开的
                            Connection existing = tempConnections.put(node.getAddress(), result); // NOTE:htt, 将result添加到tempConnections临时链接中
                            assert existing == null; // NOTE:htt, 如果existing不为null，则抛出异常
                            success = true; // NOTE:htt, 设置success为true
                        }
                    } finally {
                        if (success == false) { // NOTE:htt, 如果success为false，则关闭连接
                            logger.trace("[{}] closing connection to [{}] due to failure", id(), node);
                            IOUtils.closeWhileHandlingException(result); // NOTE:htt, 异常情况，关闭连接
                        }
                    }
                }
            }
            return result; // NOTE:htt, 返回链接结果
        }

        private void ensureOpen() { // NOTE:htt, 确保pingingRound是打开的
            if (isClosed()) {
                throw new AlreadyClosedException("pinging round [" + id + "] is finished");
            }
        }

        public void addPingResponseToCollection(PingResponse pingResponse) { // NOTE:htt, 添加ping响应到集合
            if (localNode.equals(pingResponse.node()) == false) { // NOTE:htt, 如果本地节点与ping响应的节点不同，则添加到集合
                pingCollection.addPing(pingResponse); // NOTE:htt, 添加ping响应到集合
            }
        }

        @Override
        public void close() { // NOTE:htt, 关闭pingingRound
            List<Connection> toClose = null;
            synchronized (this) {
                if (closed.compareAndSet(false, true)) { // NOTE:htt, 关闭标志设置为true
                    activePingingRounds.remove(id); // NOTE:htt, 移除pingingRound
                    toClose = new ArrayList<>(tempConnections.values()); // NOTE:htt, 获取临时连接
                    tempConnections.clear(); // NOTE:htt, 清空临时连接
                }
            }
            if (toClose != null) {
                // we actually closed
                try {
                    pingListener.accept(pingCollection); // NOTE:htt, 接受ping集合
                } finally {
                    IOUtils.closeWhileHandlingException(toClose); // NOTE:htt, 关闭临时连接
                }
            }
        }

        public ConnectionProfile getConnectionProfile() { // NOTE:htt, 获取连接配置
            return connectionProfile; // NOTE:htt, 返回连接配置
        }
    }


    protected void sendPings(final TimeValue timeout, final PingingRound pingingRound) { // NOTE:htt, 发生ping请求，超时时间默认3s
        final ClusterState lastState = contextProvider.clusterState(); // NOTE:htt, 获取最后集群状态
        final UnicastPingRequest pingRequest = new UnicastPingRequest(pingingRound.id(), timeout, createPingResponse(lastState)); // NOTE:htt, 创建ping请求，并包含当前接节点中Locla/masternode/clusterstate作为ping回包信息

        Set<DiscoveryNode> nodesFromResponses = temporalResponses.stream().map(pingResponse -> {
            assert clusterName.equals(pingResponse.clusterName()) :
                "got a ping request from a different cluster. expected " + clusterName + " got " + pingResponse.clusterName();
            return pingResponse.node();
        }).collect(Collectors.toSet()); // NOTE:htt, 获取响应的节点

        // dedup by address
        final Map<TransportAddress, DiscoveryNode> uniqueNodesByAddress =
            Stream.concat(pingingRound.getSeedNodes().stream(), nodesFromResponses.stream())
                .collect(Collectors.toMap(DiscoveryNode::getAddress, Function.identity(), (n1, n2) -> n1)); // NOTE:htt, 去重，以地址为key，节点为value


        // resolve what we can via the latest cluster state
        final Set<DiscoveryNode> nodesToPing = uniqueNodesByAddress.values().stream() // NOTE:htt, 获取发送ping的所有节点
            .map(node -> {
                DiscoveryNode foundNode = lastState.nodes().findByAddress(node.getAddress()); // NOTE:htt, 从最新集群状态中获取节点
                if (foundNode == null) {
                    return node; // NOTE:htt, 如果未找到节点，则返回原来的节点
                } else {
                    return foundNode; // NOTE:htt, 如果找到节点，则返回找到的节点
                }
            }).collect(Collectors.toSet()); // NOTE:htt, 如果找到节点，则返回找到的节点，否则返回原来的节点

        nodesToPing.forEach(node -> sendPingRequestToNode(node, timeout, pingingRound, pingRequest)); // NOTE:htt, 遍历所有待发送ping的节点，发送ping请求
    }

    private void sendPingRequestToNode(final DiscoveryNode node, TimeValue timeout, final PingingRound pingingRound,
                                       final UnicastPingRequest pingRequest) { // NOTE:htt, 发送ping请求到节点
        submitToExecutor(new AbstractRunnable() {
            @Override
            protected void doRun() throws Exception { // NOTE:htt, 发送ping请求
                Connection connection = null;
                if (transportService.nodeConnected(node)) { // NOTE:htt, 如果节点已连接，则获取连接
                    try {
                        // concurrency can still cause disconnects
                        connection = transportService.getConnection(node); // NOTE:htt, 获取连接
                    } catch (NodeNotConnectedException e) {
                        logger.trace("[{}] node [{}] just disconnected, will create a temp connection", pingingRound.id(), node);
                    }
                }

                if (connection == null) {
                    connection = pingingRound.getOrConnect(node); // NOTE:htt, 如果节点未连接，则获取或连接节点
                }

                logger.trace("[{}] sending to {}", pingingRound.id(), node);
                transportService.sendRequest(connection, ACTION_NAME, pingRequest,
                    TransportRequestOptions.builder().withTimeout((long) (timeout.millis() * 1.25)).build(), // NOTE:htt, 超时时间为默认值1.25倍(默认值3s)
                    getPingResponseHandler(pingingRound, node)); // NOTE:htt, 给每个节点发送ping请求, 并处理响应，将响应添加到pingingRound中
            }

            @Override
            public void onFailure(Exception e) { // NOTE:htt, 处理ping请求异常
                if (e instanceof ConnectTransportException || e instanceof AlreadyClosedException) {
                    // can't connect to the node - this is more common path!
                    logger.trace(() -> new ParameterizedMessage("[{}] failed to ping {}", pingingRound.id(), node), e);
                } else if (e instanceof RemoteTransportException) {
                    // something went wrong on the other side
                    logger.debug(() -> new ParameterizedMessage(
                            "[{}] received a remote error as a response to ping {}", pingingRound.id(), node), e);
                } else {
                    logger.warn(() -> new ParameterizedMessage("[{}] failed send ping to {}", pingingRound.id(), node), e);
                }
            }

            @Override
            public void onRejection(Exception e) { // NOTE:htt, 处理ping请求拒绝
                // The RejectedExecutionException can come from the fact unicastZenPingExecutorService is at its max down in sendPings
                // But don't bail here, we can retry later on after the send ping has been scheduled.
                logger.debug("Ping execution rejected", e);
            }
        });
    }

    // for testing
    protected void submitToExecutor(AbstractRunnable abstractRunnable) {
        unicastZenPingExecutorService.execute(abstractRunnable);
    }

    // for testing
    protected TransportResponseHandler<UnicastPingResponse> getPingResponseHandler(final PingingRound pingingRound,
                                                                                   final DiscoveryNode node) { // NOTE:htt, 获取pingResponse处理的handler
        return new TransportResponseHandler<UnicastPingResponse>() { // NOTE:htt, 处理ping节点列表回包，添加到pingingRound中

            @Override
            public UnicastPingResponse read(StreamInput in) throws IOException { // NOTE:htt, 读取ping节点列表回包
                return new UnicastPingResponse(in); // NOTE:htt, 创建unicast ping响应
            }

            @Override
            public String executor() { // NOTE:htt, 使用相同的线程池
                return ThreadPool.Names.SAME; // NOTE:htt, 使用相同的线程池
            }

            @Override
            public void handleResponse(UnicastPingResponse response) { // NOTE:htt, 处理ping节点列表回包，添加到pingingRound中
                logger.trace("[{}] received response from {}: {}", pingingRound.id(), node, Arrays.toString(response.pingResponses));
                if (pingingRound.isClosed()) { // NOTE:htt, 如果pingingRound已关闭，则跳过
                    if (logger.isTraceEnabled()) { // NOTE:htt, 如果日志级别为trace，则打印日志
                        logger.trace("[{}] skipping received response from {}. already closed", pingingRound.id(), node);
                    }
                } else {
                    Stream.of(response.pingResponses).forEach(pingingRound::addPingResponseToCollection);
                }
            }

            @Override
            public void handleException(TransportException exp) { // NOTE:htt, 处理ping节点列表回包异常
                if (exp instanceof ConnectTransportException || exp.getCause() instanceof ConnectTransportException ||
                    exp.getCause() instanceof AlreadyClosedException) { // NOTE:htt, 如果异常为连接异常或已关闭异常，则跳过
                    // ok, not connected...
                    logger.trace(() -> new ParameterizedMessage("failed to connect to {}", node), exp); // NOTE:htt, 打印日志
                } else if (closed == false) { // NOTE:htt, 如果closed为false，则打印日志
                    logger.warn(() -> new ParameterizedMessage("failed to send ping to [{}]", node), exp); // NOTE:htt, 打印日志
                }
            }
        };
    }

    private UnicastPingResponse handlePingRequest(final UnicastPingRequest request) { // NOTE:htt, 处理ping节点列表请求，返回当前节点包含的所有节点信息，以及请求节点中的ping回包信息
        assert clusterName.equals(request.pingResponse.clusterName()) :
            "got a ping request from a different cluster. expected " + clusterName + " got " + request.pingResponse.clusterName();
        temporalResponses.add(request.pingResponse); // NOTE:htt, 添加请求节点的ping响应到临时集合
        // add to any ongoing pinging
        activePingingRounds.values().forEach(p -> p.addPingResponseToCollection(request.pingResponse)); // NOTE:htt, 添加ping响应到集合
        threadPool.schedule(TimeValue.timeValueMillis(request.timeout.millis() * 2), ThreadPool.Names.SAME,
            () -> temporalResponses.remove(request.pingResponse)); // NOTE:htt, 2倍超时时间后，清除当前节点的回包

        List<PingResponse> pingResponses = CollectionUtils.iterableAsArrayList(temporalResponses);
        pingResponses.add(createPingResponse(contextProvider.clusterState())); // NOTE:htt, 添加当前节点ping响应到集合，即添加本地节点和master节点

        return new UnicastPingResponse(request.id, pingResponses.toArray(new PingResponse[pingResponses.size()])); // NOTE:htt, 创建unicast ping响应，包含当前有的所有节点信息
    }

    class UnicastPingRequestHandler implements TransportRequestHandler<UnicastPingRequest> { // NOTE:htt, unicast ping请求处理器

        @Override
        public void messageReceived(UnicastPingRequest request, TransportChannel channel) throws Exception { // NOTE:htt, 处理ping请求回包，返回当前节点包含所有节点信息
            if (closed) { // NOTE:htt, 如果线程已关闭，则返回node已关闭
                throw new AlreadyClosedException("node is shutting down");
            }
            if (request.pingResponse.clusterName().equals(clusterName)) { // NOTE:htt, 如果请求的clusterName与本地clusterName相同，则处理请求
                channel.sendResponse(handlePingRequest(request)); // NOTE:htt, 返回当前节点所有所有接地那信息，以及请求节点中的ping回包信息
            } else {
                throw new IllegalStateException( // NOTE:htt, 如果请求的clusterName与本地clusterName不同，则抛出异常
                    String.format(
                        Locale.ROOT,
                        "mismatched cluster names; request: [%s], local: [%s]",
                        request.pingResponse.clusterName().value(),
                        clusterName.value())); // NOTE:htt, 如果请求的clusterName与本地clusterName不同，则抛出异常
            }
        }
    }

    static class UnicastPingRequest extends TransportRequest { // NOTE:htt, unicast ping请求

        final int id; // NOTE:htt, id
        final TimeValue timeout; // NOTE:htt, 超时时间
        final PingResponse pingResponse; // NOTE: htt, ping response including clusterName/node/master/clusterStateVersion

        UnicastPingRequest(int id, TimeValue timeout, PingResponse pingResponse) {
            this.id = id;
            this.timeout = timeout;
            this.pingResponse = pingResponse;
        }

        UnicastPingRequest(StreamInput in) throws IOException {
            super(in);
            id = in.readInt();
            timeout = in.readTimeValue();
            pingResponse = new PingResponse(in);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            throw new UnsupportedOperationException("usage of Streamable is to be replaced by Writeable");
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeInt(id);
            out.writeTimeValue(timeout);
            pingResponse.writeTo(out);
        }
    }

    private PingResponse createPingResponse(ClusterState clusterState) { // NOTE:htt, 当前节点的ping响应
        DiscoveryNodes discoNodes = clusterState.nodes(); // NOTE:htt, 获取集群中所有节点
        return new PingResponse(discoNodes.getLocalNode(), discoNodes.getMasterNode(), clusterState); // NOTE:htt, 创建ping响应，包含本地节点以及master节点
    }

    static class UnicastPingResponse extends TransportResponse { // NOTE:htt, unicast ping回包

        final int id; // NOTE:htt, id

        final PingResponse[] pingResponses; // NOTE:htt, ping多个回包

        UnicastPingResponse(int id, PingResponse[] pingResponses) {
            this.id = id;
            this.pingResponses = pingResponses;
        }

        UnicastPingResponse(StreamInput in) throws IOException {
            id = in.readInt();
            pingResponses = new PingResponse[in.readVInt()];
            for (int i = 0; i < pingResponses.length; i++) {
                pingResponses[i] = new PingResponse(in);
            }
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            throw new UnsupportedOperationException("usage of Streamable is to be replaced by Writeable");
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeInt(id);
            out.writeVInt(pingResponses.length);
            for (PingResponse pingResponse : pingResponses) {
                pingResponse.writeTo(out);
            }
        }
    }

    protected Version getVersion() {
        return Version.CURRENT; // for tests
    }
}
