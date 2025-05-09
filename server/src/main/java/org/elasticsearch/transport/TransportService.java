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

package org.elasticsearch.transport;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.node.liveness.TransportLivenessAction;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.ConcurrentMapLong;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static org.elasticsearch.common.settings.Setting.listSetting;

public class TransportService extends AbstractLifecycleComponent { // NOTE: htt, 建立tcp连接并发送请求到对应节点, transportSevice to open connection to DiscoveryNode and sendRequest to node

    public static final String DIRECT_RESPONSE_PROFILE = ".direct";
    public static final String HANDSHAKE_ACTION_NAME = "internal:transport/handshake";

    private final CountDownLatch blockIncomingRequestsLatch = new CountDownLatch(1);
    protected final Transport transport; // NOTE: htt, transport的处理入口，即netty收到包之后对应处理，目前为Netty4Transport
    protected final ThreadPool threadPool;
    protected final ClusterName clusterName;
    protected final TaskManager taskManager; // NOTE: htt, task manager
    private final TransportInterceptor.AsyncSender asyncSender;
    private final Function<BoundTransportAddress, DiscoveryNode> localNodeFactory;
    private final boolean connectToRemoteCluster;

    volatile Map<String, RequestHandlerRegistry> requestHandlers = Collections.emptyMap(); // NOTE: htt，包请求处理，收到请求后，根据action执行相应的处理
    final Object requestHandlerMutex = new Object(); // NOTE: htt, requestHandler mutex

    final ConcurrentMapLong<RequestHolder> clientHandlers = ConcurrentCollections.newConcurrentMapLongWithAggressiveConcurrency(); // NOTE: htt, clientHandlers，记录请求requestId以及对应的处理

    final CopyOnWriteArrayList<TransportConnectionListener> connectionListeners = new CopyOnWriteArrayList<>(); // NOTE:htt, 链接建立完成后执行的操作

    private final TransportInterceptor interceptor; // NOTE:htt, 拦截器

    // An LRU (don't really care about concurrency here) that holds the latest timed out requests so if they
    // do show up, we can print more descriptive information about them
    final Map<Long, TimeoutInfoHolder> timeoutInfoHandlers =
        Collections.synchronizedMap(new LinkedHashMap<Long, TimeoutInfoHolder>(100, .75F, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry eldest) { // NOTE: htt, put(key,value)后执行 afterNodeInsertion() 然后会调用这里逻辑，只保留100个请求，超出的会将队首的请求移除；这里是当前请求超时后会从clientHandlers 中移到 timeoutInfoHandlers，但是 timeoutInfoHandlers并没有超时移除元素的问题，解决该map的过大的问题
                return size() > 100;
            }
        });

    public static final TransportInterceptor NOOP_TRANSPORT_INTERCEPTOR = new TransportInterceptor() {};

    // tracer log

    public static final Setting<List<String>> TRACE_LOG_INCLUDE_SETTING =
        listSetting("transport.tracer.include", emptyList(), Function.identity(), Property.Dynamic, Property.NodeScope);
    public static final Setting<List<String>> TRACE_LOG_EXCLUDE_SETTING =
        listSetting("transport.tracer.exclude", Arrays.asList("internal:discovery/zen/fd*", TransportLivenessAction.NAME),
            Function.identity(), Property.Dynamic, Property.NodeScope); // NOTE:htt, internal:discovery/zen/fd*信息不进行trace

    private final Logger tracerLog; // NOTE:htt, tracer日志

    volatile String[] tracerLogInclude; // NOTE:htt, 需要tracer的action
    volatile String[] tracerLogExclude; // NOTE:htt, 不需要tracer的action

    private final RemoteClusterService remoteClusterService; // NOTE: htt remoteCluster

    /** if set will call requests sent to this id to shortcut and executed locally */
    volatile DiscoveryNode localNode = null; // NOTE: htt, 本地节点
    private final Transport.Connection localNodeConnection = new Transport.Connection() { // NOTE: htt, localNodeConnection
        @Override
        public DiscoveryNode getNode() {
            return localNode;
        }

        @Override
        public void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options)
            throws IOException, TransportException {
            sendLocalRequest(requestId, action, request, options); // NOTE: htt, 本节点内部发送高效性
        }

        @Override
        public void close() throws IOException {
        }
    };

    /**
     * Build the service.
     *
     * @param clusterSettings if non null, the {@linkplain TransportService} will register with the {@link ClusterSettings} for settings
 *        updates for {@link #TRACE_LOG_EXCLUDE_SETTING} and {@link #TRACE_LOG_INCLUDE_SETTING}.
     */
    public TransportService(Settings settings, Transport transport, ThreadPool threadPool, TransportInterceptor transportInterceptor,
                            Function<BoundTransportAddress, DiscoveryNode> localNodeFactory, @Nullable ClusterSettings clusterSettings,
                            Set<String> taskHeaders) {
        super(settings);
        this.transport = transport;
        this.threadPool = threadPool;
        this.localNodeFactory = localNodeFactory;
        this.clusterName = ClusterName.CLUSTER_NAME_SETTING.get(settings);
        setTracerLogInclude(TRACE_LOG_INCLUDE_SETTING.get(settings));
        setTracerLogExclude(TRACE_LOG_EXCLUDE_SETTING.get(settings));
        tracerLog = Loggers.getLogger(logger, ".tracer");
        taskManager = createTaskManager(settings, threadPool, taskHeaders);
        this.interceptor = transportInterceptor;
        this.asyncSender = interceptor.interceptSender(this::sendRequestInternal);
        this.connectToRemoteCluster = RemoteClusterService.ENABLE_REMOTE_CLUSTERS.get(settings);
        remoteClusterService = new RemoteClusterService(settings, this);
        if (clusterSettings != null) {
            clusterSettings.addSettingsUpdateConsumer(TRACE_LOG_INCLUDE_SETTING, this::setTracerLogInclude);
            clusterSettings.addSettingsUpdateConsumer(TRACE_LOG_EXCLUDE_SETTING, this::setTracerLogExclude);
            if (connectToRemoteCluster) {
                remoteClusterService.listenForUpdates(clusterSettings);
            }
        }
    }

    public RemoteClusterService getRemoteClusterService() {
        return remoteClusterService;
    }

    public DiscoveryNode getLocalNode() {
        return localNode;
    }

    public TaskManager getTaskManager() {
        return taskManager;
    }

    protected TaskManager createTaskManager(Settings settings, ThreadPool threadPool, Set<String> taskHeaders) {
        return new TaskManager(settings, threadPool, taskHeaders);
    }

    /**
     * The executor service for this transport service.
     *
     * @return the executor service
     */
    protected ExecutorService getExecutorService() {
        return threadPool.generic();
    }

    void setTracerLogInclude(List<String> tracerLogInclude) {
        this.tracerLogInclude = tracerLogInclude.toArray(Strings.EMPTY_ARRAY);
    }

    void setTracerLogExclude(List<String> tracerLogExclude) {
        this.tracerLogExclude = tracerLogExclude.toArray(Strings.EMPTY_ARRAY);
    }

    @Override
    protected void doStart() { // NOTE: htt, do start transport services
        transport.setTransportService(this);
        transport.start();

        if (transport.boundAddress() != null && logger.isInfoEnabled()) {
            logger.info("{}", transport.boundAddress());
            for (Map.Entry<String, BoundTransportAddress> entry : transport.profileBoundAddresses().entrySet()) {
                logger.info("profile [{}]: {}", entry.getKey(), entry.getValue());
            }
        }
        localNode = localNodeFactory.apply(transport.boundAddress());
        registerRequestHandler(  // NOTE: htt, register handshakeAction
            HANDSHAKE_ACTION_NAME, // NOTE: htt, 心跳协议回包
            () -> HandshakeRequest.INSTANCE,
            ThreadPool.Names.SAME,
            false, false,
            (request, channel) -> channel.sendResponse(
                    new HandshakeResponse(localNode, clusterName, localNode.getVersion())));
        if (connectToRemoteCluster) {
            // here we start to connect to the remote clusters
            remoteClusterService.initializeRemoteClusters();
        }
    }

    @Override
    protected void doStop() {
        try {
            transport.stop();
        } finally {
            // in case the transport is not connected to our local node (thus cleaned on node disconnect)
            // make sure to clean any leftover on going handles
            for (Map.Entry<Long, RequestHolder> entry : clientHandlers.entrySet()) {
                final RequestHolder holderToNotify = clientHandlers.remove(entry.getKey());
                if (holderToNotify != null) {
                    // callback that an exception happened, but on a different thread since we don't
                    // want handlers to worry about stack overflows
                    getExecutorService().execute(new AbstractRunnable() {
                        @Override
                        public void onRejection(Exception e) {
                            // if we get rejected during node shutdown we don't wanna bubble it up
                            logger.debug(
                                () -> new ParameterizedMessage(
                                    "failed to notify response handler on rejection, action: {}",
                                    holderToNotify.action()),
                                e);
                        }
                        @Override
                        public void onFailure(Exception e) {
                            logger.warn(
                                () -> new ParameterizedMessage(
                                    "failed to notify response handler on exception, action: {}",
                                    holderToNotify.action()),
                                e);
                        }
                        @Override
                        public void doRun() {
                            TransportException ex = new TransportException("transport stopped, action: " + holderToNotify.action());
                            holderToNotify.handler().handleException(ex);
                        }
                    });
                }
            }
        }
    }

    @Override
    protected void doClose() throws IOException {
        IOUtils.close(remoteClusterService, transport);
    }

    /**
     * start accepting incoming requests.
     * when the transport layer starts up it will block any incoming requests until
     * this method is called
     */
    public final void acceptIncomingRequests() {
        blockIncomingRequestsLatch.countDown();
    }

    public TransportInfo info() {
        BoundTransportAddress boundTransportAddress = boundAddress();
        if (boundTransportAddress == null) {
            return null;
        }
        return new TransportInfo(boundTransportAddress, transport.profileBoundAddresses());
    }

    public TransportStats stats() {
        return transport.getStats();
    }

    public BoundTransportAddress boundAddress() {
        return transport.boundAddress();
    }

    public List<String> getLocalAddresses() {
        return transport.getLocalAddresses();
    }

    /**
     * Returns <code>true</code> iff the given node is already connected.
     */
    public boolean nodeConnected(DiscoveryNode node) {
        return isLocalNode(node) || transport.nodeConnected(node);
    }

    public void connectToNode(DiscoveryNode node) throws ConnectTransportException {
        connectToNode(node, null);
    }

    /**
     * Connect to the specified node with the given connection profile
     *
     * @param node the node to connect to
     * @param connectionProfile the connection profile to use when connecting to this node
     */
    public void connectToNode(final DiscoveryNode node, ConnectionProfile connectionProfile) { // NOTE:htt, 连接到对端节点
        if (isLocalNode(node)) { // NOTE: htt, local node just return
            return;
        }
        transport.connectToNode(node, connectionProfile, (newConnection, actualProfile) -> {
            // We don't validate cluster names to allow for tribe node connections.
            final DiscoveryNode remote = handshake(newConnection, actualProfile.getHandshakeTimeout().millis(), cn -> true).discoveryNode;
            if (node.equals(remote) == false) {
                throw new ConnectTransportException(node, "handshake failed. unexpected remote node " + remote);
            }
        });
    }

    /**
     * Establishes and returns a new connection to the given node. The connection is NOT maintained by this service, it's the callers
     * responsibility to close the connection once it goes out of scope.
     * @param node the node to connect to
     * @param profile the connection profile to use
     */
    public Transport.Connection openConnection(final DiscoveryNode node, ConnectionProfile profile) throws IOException {
        if (isLocalNode(node)) {
            return localNodeConnection; // NOTE: htt, 本节点内部连接
        } else {
            return transport.openConnection(node, profile); // NOTE: htt, TcpTransport.openConnection
        }
    }

    /**
     * Executes a high-level handshake using the given connection
     * and returns the discovery node of the node the connection
     * was established with. The handshake will fail if the cluster
     * name on the target node mismatches the local cluster name.
     *
     * @param connection       the connection to a specific node
     * @param handshakeTimeout handshake timeout
     * @return the connected node
     * @throws ConnectTransportException if the connection failed
     * @throws IllegalStateException if the handshake failed
     */
    public DiscoveryNode handshake(
            final Transport.Connection connection,
            final long handshakeTimeout) throws ConnectTransportException {
        return handshake(connection, handshakeTimeout, clusterName::equals).discoveryNode;
    }

    /**
     * Executes a high-level handshake using the given connection
     * and returns the discovery node of the node the connection
     * was established with. The handshake will fail if the cluster
     * name on the target node doesn't match the local cluster name.
     *
     * @param connection       the connection to a specific node
     * @param handshakeTimeout handshake timeout
     * @param clusterNamePredicate cluster name validation predicate
     * @return the handshake response
     * @throws ConnectTransportException if the connection failed
     * @throws IllegalStateException if the handshake failed
     */
    public HandshakeResponse handshake(
        final Transport.Connection connection,
        final long handshakeTimeout, Predicate<ClusterName> clusterNamePredicate) throws ConnectTransportException {
        final HandshakeResponse response;
        final DiscoveryNode node = connection.getNode();
        try {
            PlainTransportFuture<HandshakeResponse> futureHandler = new PlainTransportFuture<>(
                new FutureTransportResponseHandler<HandshakeResponse>() {
                @Override
                public HandshakeResponse newInstance() {
                    return new HandshakeResponse();
                }
            });
            sendRequest(connection, HANDSHAKE_ACTION_NAME, HandshakeRequest.INSTANCE,
                TransportRequestOptions.builder().withTimeout(handshakeTimeout).build(), futureHandler); // NOTE: htt, async send request
            response = futureHandler.txGet(); // NOTE: htt, 同步等待状态获取，知道请求超时
        } catch (Exception e) {
            throw new IllegalStateException("handshake failed with " + node, e);
        }

        if (!clusterNamePredicate.test(response.clusterName)) { // NOTE: htt, not same clusterName then return
            throw new IllegalStateException("handshake failed, mismatched cluster name [" + response.clusterName + "] - " + node);
        } else if (response.version.isCompatible(localNode.getVersion()) == false) {
            throw new IllegalStateException("handshake failed, incompatible version [" + response.version + "] - " + node);
        }

        return response;
    }

    static class HandshakeRequest extends TransportRequest { // NOTE; htt, handshakeRequest

        public static final HandshakeRequest INSTANCE = new HandshakeRequest();

        private HandshakeRequest() {
        }

    }

    public static class HandshakeResponse extends TransportResponse { // NOTE: htt, handshake  response
        private DiscoveryNode discoveryNode;
        private ClusterName clusterName;
        private Version version;

        HandshakeResponse() {
        }

        public HandshakeResponse(DiscoveryNode discoveryNode, ClusterName clusterName, Version version) {
            this.discoveryNode = discoveryNode;
            this.version = version;
            this.clusterName = clusterName;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            discoveryNode = in.readOptionalWriteable(DiscoveryNode::new);
            clusterName = new ClusterName(in);
            version = Version.readVersion(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalWriteable(discoveryNode);
            clusterName.writeTo(out);
            Version.writeVersion(version, out);
        }

        public DiscoveryNode getDiscoveryNode() {
            return discoveryNode;
        }

        public ClusterName getClusterName() {
            return clusterName;
        }
    }

    public void disconnectFromNode(DiscoveryNode node) {
        if (isLocalNode(node)) {
            return;
        }
        transport.disconnectFromNode(node);
    }

    public void addConnectionListener(TransportConnectionListener listener) {
        connectionListeners.add(listener);
    }

    public void removeConnectionListener(TransportConnectionListener listener) {
        connectionListeners.remove(listener);
    }

    public <T extends TransportResponse> TransportFuture<T> submitRequest(DiscoveryNode node, String action, TransportRequest request,
                                                                          TransportResponseHandler<T> handler) throws TransportException {
        return submitRequest(node, action, request, TransportRequestOptions.EMPTY, handler);
    }

    public <T extends TransportResponse> TransportFuture<T> submitRequest(DiscoveryNode node, String action, TransportRequest request,
                                                                          TransportRequestOptions options,
                                                                          TransportResponseHandler<T> handler) throws TransportException {
        PlainTransportFuture<T> futureHandler = new PlainTransportFuture<>(handler);
        try {
            Transport.Connection connection = getConnection(node);
            sendRequest(connection, action, request, options, futureHandler);
        } catch (NodeNotConnectedException ex) {
            // the caller might not handle this so we invoke the handler
            futureHandler.handleException(ex);
        }
        return futureHandler;
    }

    public <T extends TransportResponse> void sendRequest(final DiscoveryNode node, final String action,
                                                                final TransportRequest request,
                                                                final TransportResponseHandler<T> handler) {
        try {
            Transport.Connection connection = getConnection(node);
            sendRequest(connection, action, request, TransportRequestOptions.EMPTY, handler);
        } catch (NodeNotConnectedException ex) {
            // the caller might not handle this so we invoke the handler
            handler.handleException(ex);
        }
    }

    public final <T extends TransportResponse> void sendRequest(final DiscoveryNode node, final String action,
                                                                final TransportRequest request,
                                                                final TransportRequestOptions options,
                                                                TransportResponseHandler<T> handler) {
        try {
            Transport.Connection connection = getConnection(node);
            sendRequest(connection, action, request, options, handler);
        } catch (NodeNotConnectedException ex) {
            // the caller might not handle this so we invoke the handler
            handler.handleException(ex);
        }
    }

    public final <T extends TransportResponse> void sendRequest(final Transport.Connection connection, final String action,
                                                                final TransportRequest request,
                                                                final TransportRequestOptions options,
                                                                TransportResponseHandler<T> handler) {

        asyncSender.sendRequest(connection, action, request, options, handler); // NOTE: htt, sendRequestInternal
    }

    /**
     * Returns either a real transport connection or a local node connection if we are using the local node optimization.
     * @throws NodeNotConnectedException if the given node is not connected
     */
    public Transport.Connection getConnection(DiscoveryNode node) {
        if (isLocalNode(node)) { // NOTE: htt, return local node connection
            return localNodeConnection;
        } else {
            return transport.getConnection(node);
        }
    }

    public final <T extends TransportResponse> void sendChildRequest(final DiscoveryNode node, final String action,
                                                                     final TransportRequest request, final Task parentTask,
                                                                     final TransportRequestOptions options,
                                                                     final TransportResponseHandler<T> handler) {
        try {
            Transport.Connection connection = getConnection(node);
            sendChildRequest(connection, action, request, parentTask, options, handler);
        } catch (NodeNotConnectedException ex) {
            // the caller might not handle this so we invoke the handler
            handler.handleException(ex);
        }
    }

    public <T extends TransportResponse> void sendChildRequest(final Transport.Connection connection, final String action,
                                                               final TransportRequest request, final Task parentTask,
                                                               final TransportResponseHandler<T> handler) {
        sendChildRequest(connection, action, request, parentTask, TransportRequestOptions.EMPTY, handler);
    }

    public <T extends TransportResponse> void sendChildRequest(final Transport.Connection connection, final String action,
                                                               final TransportRequest request, final Task parentTask,
                                                               final TransportRequestOptions options,
                                                               final TransportResponseHandler<T> handler) {
        request.setParentTask(localNode.getId(), parentTask.getId());
        try {
            sendRequest(connection, action, request, options, handler);
        } catch (TaskCancelledException ex) {
            // The parent task is already cancelled - just fail the request
            handler.handleException(new TransportException(ex));
        } catch (NodeNotConnectedException ex) {
            // the caller might not handle this so we invoke the handler
            handler.handleException(ex);
        }

    }

    private <T extends TransportResponse> void sendRequestInternal(final Transport.Connection connection, final String action,
                                                                   final TransportRequest request,
                                                                   final TransportRequestOptions options,
                                                                   TransportResponseHandler<T> handler) {
        if (connection == null) {
            throw new IllegalStateException("can't send request to a null connection");
        }
        DiscoveryNode node = connection.getNode();
        final long requestId = transport.newRequestId();
        final TimeoutHandler timeoutHandler;
        try {
        	logger.debug("[outer-2, ]action:" + action + " ; request:"+ request);
            if (options.timeout() == null) {
            	logger.debug("[in-2, ]action:" + action + " ; request:"+ request + " ; no timeout");
                timeoutHandler = null;
            } else {
            	logger.debug("[in-2, ]action:" + action + " ; request:"+ request + " ; has timeout");
                timeoutHandler = new TimeoutHandler(requestId);
            }
            Supplier<ThreadContext.StoredContext> storedContextSupplier = threadPool.getThreadContext().newRestorableContext(true);
            TransportResponseHandler<T> responseHandler = new ContextRestoreResponseHandler<>(storedContextSupplier, handler);
            clientHandlers.put(requestId, new RequestHolder<>(responseHandler, connection, action, timeoutHandler)); // NOTE: htt, es侧用于异步请求管理，先保存待回包后根据 requestId来获取到对应的请求，以便继续处理
            if (lifecycle.stoppedOrClosed()) {
                // if we are not started the exception handling will remove the RequestHolder again and calls the handler to notify
                // the caller. It will only notify if the toStop code hasn't done the work yet.
                throw new TransportException("TransportService is closed stopped can't send request");
            }
            if (timeoutHandler != null) {
                assert options.timeout() != null;
                logger.debug("[in-3, ]action:" + action + " ; request:"+ request + " ; has timeout, and has been scheduled");
                timeoutHandler.future = threadPool.schedule(options.timeout(), ThreadPool.Names.GENERIC, timeoutHandler); // NOTE: htt，带有超时时间请求加入时间handler监控处理
            }
            connection.sendRequest(requestId, action, request, options); // local node optimization happens upstream
        } catch (final Exception e) {
            // usually happen either because we failed to connect to the node
            // or because we failed serializing the message
            final RequestHolder holderToNotify = clientHandlers.remove(requestId);
            // If holderToNotify == null then handler has already been taken care of.
            if (holderToNotify != null) {
                holderToNotify.cancelTimeout();
                // callback that an exception happened, but on a different thread since we don't
                // want handlers to worry about stack overflows
                final SendRequestTransportException sendRequestException = new SendRequestTransportException(node, action, e);
                threadPool.executor(ThreadPool.Names.GENERIC).execute(new AbstractRunnable() {
                    @Override
                    public void onRejection(Exception e) {
                        // if we get rejected during node shutdown we don't wanna bubble it up
                        logger.debug(
                            () -> new ParameterizedMessage(
                                "failed to notify response handler on rejection, action: {}",
                                holderToNotify.action()),
                            e);
                    }
                    @Override
                    public void onFailure(Exception e) {
                        logger.warn(
                            () -> new ParameterizedMessage(
                                "failed to notify response handler on exception, action: {}",
                                holderToNotify.action()),
                            e);
                    }
                    @Override
                    protected void doRun() throws Exception {
                        holderToNotify.handler().handleException(sendRequestException);
                    }
                });
            } else {
                logger.debug("Exception while sending request, handler likely already notified due to timeout", e);
            }
        }
    }
    // NOTE: htt, 如果是给本节点发送请求，则直接找到对应action，然后执行其中的handler
    private void sendLocalRequest(long requestId, final String action, final TransportRequest request, TransportRequestOptions options) {
        final DirectResponseChannel channel = new DirectResponseChannel(logger, localNode, action, requestId, this, threadPool);
        try {
            onRequestSent(localNode, requestId, action, request, options);
            onRequestReceived(requestId, action); // NOTE: htt, 阻塞直到可以接受请求
            final RequestHandlerRegistry reg = getRequestHandler(action); // NOTE: htt, 本地请求，直接找到对应action的处理
            if (reg == null) {
                throw new ActionNotFoundTransportException("Action [" + action + "] not found");
            }
            final String executor = reg.getExecutor();
            if (ThreadPool.Names.SAME.equals(executor)) { // NOTE: htt, same threadPool executor
                //noinspection unchecked
                reg.processMessageReceived(request, channel);
            } else {
                threadPool.executor(executor).execute(new AbstractRunnable() { // NOTE: htt, threadPool executor
                    @Override
                    protected void doRun() throws Exception {
                        //noinspection unchecked
                        reg.processMessageReceived(request, channel); // NOTE: htt, 新启动线程执行
                    }

                    @Override
                    public boolean isForceExecution() {
                        return reg.isForceExecution();
                    }

                    @Override
                    public void onFailure(Exception e) {
                        try {
                            channel.sendResponse(e);
                        } catch (Exception inner) {
                            inner.addSuppressed(e);
                            logger.warn(() -> new ParameterizedMessage(
                                    "failed to notify channel of error message for action [{}]", action), inner);
                        }
                    }
                });
            }

        } catch (Exception e) {
            try {
                channel.sendResponse(e);
            } catch (Exception inner) {
                inner.addSuppressed(e);
                logger.warn(
                    () -> new ParameterizedMessage(
                        "failed to notify channel of error message for action [{}]", action), inner);
            }
        }
    }

    private boolean shouldTraceAction(String action) { // NOTE:htt, 是否需要进行tracer
        if (tracerLogInclude.length > 0) {
            if (Regex.simpleMatch(tracerLogInclude, action) == false) { // NOTE:htt, 如果有tracer的action，但是当前不在其中则不进行tracer
                return false;
            }
        }
        if (tracerLogExclude.length > 0) {
            return !Regex.simpleMatch(tracerLogExclude, action); // NOTE:htt, 如果有exclude的tracer的action，并且在其中则不进行tracer
        }
        return true;
    }

    public TransportAddress[] addressesFromString(String address, int perAddressLimit) throws UnknownHostException {
        return transport.addressesFromString(address, perAddressLimit);
    }

    /**
     * Registers a new request handler
     *
     * @param action         The action the request handler is associated with
     * @param requestFactory a callable to be used construct new instances for streaming
     * @param executor       The executor the request handling will be executed on
     * @param handler        The handler itself that implements the request handling
     */
    public <Request extends TransportRequest> void registerRequestHandler(String action, Supplier<Request> requestFactory,
                                                    String executor, TransportRequestHandler<Request> handler) {
        handler = interceptor.interceptHandler(action, executor, false, handler);
        RequestHandlerRegistry<Request> reg = new RequestHandlerRegistry<>(
            action, Streamable.newWriteableReader(requestFactory), taskManager, handler, executor, false, true);
        registerRequestHandler(reg);
    }

    /**
     * Registers a new request handler
     *
     * @param action         The action the request handler is associated with
     * @param requestReader  a callable to be used construct new instances for streaming
     * @param executor       The executor the request handling will be executed on
     * @param handler        The handler itself that implements the request handling
     */
    public <Request extends TransportRequest> void registerRequestHandler(String action, String executor,
                                                                          Writeable.Reader<Request> requestReader,
                                                                          TransportRequestHandler<Request> handler) {
        handler = interceptor.interceptHandler(action, executor, false, handler);
        RequestHandlerRegistry<Request> reg = new RequestHandlerRegistry<>(
            action, requestReader, taskManager, handler, executor, false, true);
        registerRequestHandler(reg);
    }

    /**
     * Registers a new request handler
     *
     * @param action                The action the request handler is associated with
     * @param request               The request class that will be used to construct new instances for streaming
     * @param executor              The executor the request handling will be executed on
     * @param forceExecution        Force execution on the executor queue and never reject it
     * @param canTripCircuitBreaker Check the request size and raise an exception in case the limit is breached.
     * @param handler               The handler itself that implements the request handling
     */
    public <Request extends TransportRequest> void registerRequestHandler(String action, Supplier<Request> request,
                                                                          String executor, boolean forceExecution,
                                                                          boolean canTripCircuitBreaker,
                                                                          TransportRequestHandler<Request> handler) {
        handler = interceptor.interceptHandler(action, executor, forceExecution, handler);
        RequestHandlerRegistry<Request> reg = new RequestHandlerRegistry<>(
            action, Streamable.newWriteableReader(request), taskManager, handler, executor, forceExecution, canTripCircuitBreaker);
        registerRequestHandler(reg);
    }

    /**
     * Registers a new request handler
     *
     * @param action                The action the request handler is associated with
     * @param requestReader               The request class that will be used to construct new instances for streaming
     * @param executor              The executor the request handling will be executed on
     * @param forceExecution        Force execution on the executor queue and never reject it
     * @param canTripCircuitBreaker Check the request size and raise an exception in case the limit is breached.
     * @param handler               The handler itself that implements the request handling
     */
    public <Request extends TransportRequest> void registerRequestHandler(String action,
                                                                          String executor, boolean forceExecution,
                                                                          boolean canTripCircuitBreaker,
                                                                          Writeable.Reader<Request> requestReader,
                                                                          TransportRequestHandler<Request> handler) {
        handler = interceptor.interceptHandler(action, executor, forceExecution, handler);
        RequestHandlerRegistry<Request> reg = new RequestHandlerRegistry<>(
            action, requestReader, taskManager, handler, executor, forceExecution, canTripCircuitBreaker);
        registerRequestHandler(reg);
    }

    private <Request extends TransportRequest> void registerRequestHandler(RequestHandlerRegistry<Request> reg) {
        synchronized (requestHandlerMutex) { // NOTE: htt, lock then register request to requestHandlers
            if (requestHandlers.containsKey(reg.getAction())) { // NOTE: htt, 每个action只能注册一次
                throw new IllegalArgumentException("transport handlers for action " + reg.getAction() + " is already registered");
            }
            requestHandlers = MapBuilder.newMapBuilder(requestHandlers).put(reg.getAction(), reg).immutableMap();
        }
    }

    /** called by the {@link Transport} implementation once a request has been sent */
    void onRequestSent(DiscoveryNode node, long requestId, String action, TransportRequest request,
                       TransportRequestOptions options) {
        if (traceEnabled() && shouldTraceAction(action)) { // NOTE:htt, 判断是否需要进行tracer
            traceRequestSent(node, requestId, action, options);
        }
    }

    protected boolean traceEnabled() {
        return tracerLog.isTraceEnabled();
    }

    /** called by the {@link Transport} implementation once a response was sent to calling node */
    void onResponseSent(long requestId, String action, TransportResponse response, TransportResponseOptions options) {
        if (traceEnabled() && shouldTraceAction(action)) {
            traceResponseSent(requestId, action);
        }
    }

    /** called by the {@link Transport} implementation after an exception was sent as a response to an incoming request */
    void onResponseSent(long requestId, String action, Exception e) {
        if (traceEnabled() && shouldTraceAction(action)) {
            traceResponseSent(requestId, action, e);
        }
    }

    protected void traceResponseSent(long requestId, String action, Exception e) {
        tracerLog.trace(() -> new ParameterizedMessage("[{}][{}] sent error response", requestId, action), e);
    }

    /**
     * called by the {@link Transport} implementation when an incoming request arrives but before
     * any parsing of it has happened (with the exception of the requestId and action)
     */
    void onRequestReceived(long requestId, String action) {
        try {
            blockIncomingRequestsLatch.await();
        } catch (InterruptedException e) {
            logger.trace("interrupted while waiting for incoming requests block to be removed");
        }
        if (traceEnabled() && shouldTraceAction(action)) {
            traceReceivedRequest(requestId, action);
        }
    }

    public RequestHandlerRegistry getRequestHandler(String action) {
        return requestHandlers.get(action); // NOTE: htt, 获取action对应的请求处理
    }

    /**
     * called by the {@link Transport} implementation when a response or an exception has been received for a previously
     * sent request (before any processing or deserialization was done). Returns the appropriate response handler or null if not
     * found.
     */
    public TransportResponseHandler onResponseReceived(final long requestId) { // NOTE: htt, 执行回包操作
        RequestHolder holder = clientHandlers.remove(requestId);

        if (holder == null) {
            checkForTimeout(requestId);
            return null;
        }
        holder.cancelTimeout();
        if (traceEnabled() && shouldTraceAction(holder.action())) { // NOTE:htt, tracer收包信息
            traceReceivedResponse(requestId, holder.connection().getNode(), holder.action());
        }
        return holder.handler();
    }

    private void checkForTimeout(long requestId) {
        // lets see if its in the timeout holder, but sync on mutex to make sure any ongoing timeout handling has finished
        final DiscoveryNode sourceNode;
        final String action;
        assert clientHandlers.get(requestId) == null;
        TimeoutInfoHolder timeoutInfoHolder = timeoutInfoHandlers.remove(requestId); // NOTE: htt, get timeout handler
        if (timeoutInfoHolder != null) {
            long time = System.currentTimeMillis();
            logger.warn("Received response for a request that has timed out, sent [{}ms] ago, timed out [{}ms] ago, " +
                    "action [{}], node [{}], id [{}]", time - timeoutInfoHolder.sentTime(), time - timeoutInfoHolder.timeoutTime(),
                timeoutInfoHolder.action(), timeoutInfoHolder.node(), requestId);
            action = timeoutInfoHolder.action();
            sourceNode = timeoutInfoHolder.node();
        } else {
            logger.warn("Transport response handler not found of id [{}]", requestId);
            action = null;
            sourceNode = null;
        }
        // call tracer out of lock
        if (traceEnabled() == false) {
            return;
        }
        if (action == null) {
            assert sourceNode == null;
            traceUnresolvedResponse(requestId);
        } else if (shouldTraceAction(action)) {
            traceReceivedResponse(requestId, sourceNode, action);
        }
    }

    void onNodeConnected(final DiscoveryNode node) { // NOTE:htt, 链接建立完成操作
        // capture listeners before spawning the background callback so the following pattern won't trigger a call
        // connectToNode(); connection is completed successfully
        // addConnectionListener(); this listener shouldn't be called
        final Stream<TransportConnectionListener> listenersToNotify = TransportService.this.connectionListeners.stream();
        getExecutorService().execute(() -> listenersToNotify.forEach(listener -> listener.onNodeConnected(node)));
    }

    void onConnectionOpened(Transport.Connection connection) {
        // capture listeners before spawning the background callback so the following pattern won't trigger a call
        // connectToNode(); connection is completed successfully
        // addConnectionListener(); this listener shouldn't be called
        final Stream<TransportConnectionListener> listenersToNotify = TransportService.this.connectionListeners.stream();
        getExecutorService().execute(() -> listenersToNotify.forEach(listener -> listener.onConnectionOpened(connection)));
    }

    public void onNodeDisconnected(final DiscoveryNode node) { // NOTE:htt, 连接断开后执行相应监听处理
        try {
            getExecutorService().execute( () -> {
                for (final TransportConnectionListener connectionListener : connectionListeners) {
                    connectionListener.onNodeDisconnected(node); // NOTE: htt, 执行connectionListener在当前节点上进行 disconnected(), ${FaultDetection#onNodeDisconnected}
                }
            });
        } catch (EsRejectedExecutionException ex) {
            logger.debug("Rejected execution on NodeDisconnected", ex);
        }
    }

    void onConnectionClosed(Transport.Connection connection) { // NOTE: htt, 连接关闭后，会释放该连接上的对应的对象，此时不会该对象上剩余未回复的链接则不予考虑
        try {
            for (Map.Entry<Long, RequestHolder> entry : clientHandlers.entrySet()) {
                RequestHolder holder = entry.getValue();
                if (holder.connection().getCacheKey().equals(connection.getCacheKey())) { // NOTE: htt, 判断是否为同一个Connection，即 NodeChannels
                    final RequestHolder holderToNotify = clientHandlers.remove(entry.getKey()); // NOTE: htt, 取出连接上的请求，并执行连接关闭处理
                    if (holderToNotify != null) {
                        // callback that an exception happened, but on a different thread since we don't
                        // want handlers to worry about stack overflows
                        getExecutorService().execute(() -> holderToNotify.handler().handleException(new NodeDisconnectedException(
                            connection.getNode(), holderToNotify.action()))); // NOTE: htt, 该连接上的请求，执行连接关闭的异常清理处理
                    }
                }
            }
        } catch (EsRejectedExecutionException ex) {
            logger.debug("Rejected execution on onConnectionClosed", ex);
        }
    }

    protected void traceReceivedRequest(long requestId, String action) {
        tracerLog.trace("[{}][{}] received request", requestId, action);
    }

    protected void traceResponseSent(long requestId, String action) {
        tracerLog.trace("[{}][{}] sent response", requestId, action);
    }

    protected void traceReceivedResponse(long requestId, DiscoveryNode sourceNode, String action) {
        tracerLog.trace("[{}][{}] received response from [{}]", requestId, action, sourceNode);
    }

    protected void traceUnresolvedResponse(long requestId) {
        tracerLog.trace("[{}] received response but can't resolve it to a request", requestId);
    }

    protected void traceRequestSent(DiscoveryNode node, long requestId, String action, TransportRequestOptions options) {
        tracerLog.trace("[{}][{}] sent to [{}] (timeout: [{}])", requestId, action, node, options.timeout()); // NOTE:htt, 跟踪发送请求，包括 requestId, action, timeout信息
    }

    class TimeoutHandler implements Runnable { // NOTE: htt, timeoutHandler of one request

        private final long requestId;

        private final long sentTime = System.currentTimeMillis();

        volatile ScheduledFuture future;

        TimeoutHandler(long requestId) {
            this.requestId = requestId;
        }

        @Override
        public void run() { // NOTE: htt, 超时之后执行这里操作； 1. requestId从clientHandlers获取对象，2. 将对象放入到 timeoutInfoHandlers， 3. 将对象从clientHandlers移除并执行超时（如果此时没有收到回包）
            // we get first to make sure we only add the TimeoutInfoHandler if needed.
            final RequestHolder holder = clientHandlers.get(requestId);
            if (holder != null) {
                // add it to the timeout information holder, in case we are going to get a response later
                long timeoutTime = System.currentTimeMillis();
                timeoutInfoHandlers.put(requestId, new TimeoutInfoHolder(holder.connection().getNode(), holder.action(), sentTime,
                    timeoutTime));
                // now that we have the information visible via timeoutInfoHandlers, we try to remove the request id
                final RequestHolder removedHolder = clientHandlers.remove(requestId);
                if (removedHolder != null) {
                    assert removedHolder == holder : "two different holder instances for request [" + requestId + "]";
                    removedHolder.handler().handleException( // NOTE: htt, 超时后 执行 RequestHolder 中传入的 TransportResponseHandler.handleException
                        new ReceiveTimeoutTransportException(holder.connection().getNode(), holder.action(),
                            "request_id [" + requestId + "] timed out after [" + (timeoutTime - sentTime) + "ms]"));
                } else {
                    // response was processed, remove timeout info.
                    timeoutInfoHandlers.remove(requestId);
                }
            }
        }

        /**
         * cancels timeout handling. this is a best effort only to avoid running it. remove the requestId from {@link #clientHandlers}
         * to make sure this doesn't run.
         */
        public void cancel() {
            assert clientHandlers.get(requestId) == null :
                "cancel must be called after the requestId [" + requestId + "] has been removed from clientHandlers";
            FutureUtils.cancel(future);
        }
    }

    static class TimeoutInfoHolder { // NOTE: htt, timeoutInfoHoler of discoveryNode from sendTime to timeoutTime

        private final DiscoveryNode node;
        private final String action;
        private final long sentTime;
        private final long timeoutTime;

        TimeoutInfoHolder(DiscoveryNode node, String action, long sentTime, long timeoutTime) {
            this.node = node;
            this.action = action;
            this.sentTime = sentTime;
            this.timeoutTime = timeoutTime;
        }

        public DiscoveryNode node() {
            return node;
        }

        public String action() {
            return action;
        }

        public long sentTime() {
            return sentTime;
        }

        public long timeoutTime() {
            return timeoutTime;
        }
    }

    static class RequestHolder<T extends TransportResponse> { // NOTE: htt, requestHolder

        private final TransportResponseHandler<T> handler; // NOTE: htt, transportResponseHandler

        private final Transport.Connection connection;

        private final String action;

        private final TimeoutHandler timeoutHandler;

        RequestHolder(TransportResponseHandler<T> handler, Transport.Connection connection, String action, TimeoutHandler timeoutHandler) {
            this.handler = handler;
            this.connection = connection;
            this.action = action;
            this.timeoutHandler = timeoutHandler;
        }

        public TransportResponseHandler<T> handler() {
            return handler;
        }

        public Transport.Connection connection() {
            return this.connection;
        }

        public String action() {
            return this.action;
        }

        public void cancelTimeout() {
            if (timeoutHandler != null) {
                timeoutHandler.cancel();
            }
        }
    }

    /**
     * This handler wrapper ensures that the response thread executes with the correct thread context. Before any of the handle methods
     * are invoked we restore the context.
     */
    public static final class ContextRestoreResponseHandler<T extends TransportResponse> implements TransportResponseHandler<T> { // NOTE: htt, contextRestoreResponseHandler which execute by delegate

        private final TransportResponseHandler<T> delegate;
        private final Supplier<ThreadContext.StoredContext> contextSupplier;

        public ContextRestoreResponseHandler(Supplier<ThreadContext.StoredContext> contextSupplier, TransportResponseHandler<T> delegate) {
            this.delegate = delegate;
            this.contextSupplier = contextSupplier;
        }

        @Override
        public T read(StreamInput in) throws IOException {
            return delegate.read(in);
        }

        @Override
        public void handleResponse(T response) {
            try (ThreadContext.StoredContext ignore = contextSupplier.get()) {
                delegate.handleResponse(response);
            }
        }

        @Override
        public void handleException(TransportException exp) {
            try (ThreadContext.StoredContext ignore = contextSupplier.get()) {
                delegate.handleException(exp);
            }
        }

        @Override
        public String executor() {
            return delegate.executor();
        }

        @Override
        public String toString() {
            return getClass().getName() + "/" + delegate.toString();
        }

    }

    static class DirectResponseChannel implements TransportChannel { // NOTE; htt, directResponseChannel which sendResponse using TransportResponseHandler from service
        final Logger logger;
        final DiscoveryNode localNode;
        private final String action;
        private final long requestId;
        final TransportService service;
        final ThreadPool threadPool;

        DirectResponseChannel(Logger logger, DiscoveryNode localNode, String action, long requestId, TransportService service,
                              ThreadPool threadPool) {
            this.logger = logger;
            this.localNode = localNode;
            this.action = action;
            this.requestId = requestId;
            this.service = service;
            this.threadPool = threadPool;
        }

        @Override
        public String getProfileName() {
            return DIRECT_RESPONSE_PROFILE;
        }

        @Override
        public void sendResponse(TransportResponse response) throws IOException {
            sendResponse(response, TransportResponseOptions.EMPTY);
        }

        @Override
        public void sendResponse(final TransportResponse response, TransportResponseOptions options) throws IOException {
            service.onResponseSent(requestId, action, response, options);
            final TransportResponseHandler handler = service.onResponseReceived(requestId);
            // ignore if its null, the service logs it
            if (handler != null) {
                final String executor = handler.executor();
                if (ThreadPool.Names.SAME.equals(executor)) {
                    processResponse(handler, response);
                } else {
                    threadPool.executor(executor).execute(() -> processResponse(handler, response));
                }
            }
        }

        @SuppressWarnings("unchecked")
        protected void processResponse(TransportResponseHandler handler, TransportResponse response) {
            try {
                handler.handleResponse(response);
            } catch (Exception e) {
                processException(handler, wrapInRemote(new ResponseHandlerFailureTransportException(e)));
            }
        }

        @Override
        public void sendResponse(Exception exception) throws IOException {
            service.onResponseSent(requestId, action, exception); // NOTE: htt, trace log
            final TransportResponseHandler handler = service.onResponseReceived(requestId); // NOTE: htt, trace log
            // ignore if its null, the service logs it
            if (handler != null) {
                final RemoteTransportException rtx = wrapInRemote(exception);
                final String executor = handler.executor();
                if (ThreadPool.Names.SAME.equals(executor)) {
                    processException(handler, rtx); // NOTE: htt, real execute
                } else {
                    threadPool.executor(handler.executor()).execute(() -> processException(handler, rtx)); // NOTE: htt, real execute
                }
            }
        }

        protected RemoteTransportException wrapInRemote(Exception e) {
            if (e instanceof RemoteTransportException) {
                return (RemoteTransportException) e;
            }
            return new RemoteTransportException(localNode.getName(), localNode.getAddress(), action, e);
        }

        protected void processException(final TransportResponseHandler handler, final RemoteTransportException rtx) {
            try {
                handler.handleException(rtx);
            } catch (Exception e) {
                logger.error(
                    () -> new ParameterizedMessage(
                        "failed to handle exception for action [{}], handler [{}]", action, handler), e);
            }
        }

        @Override
        public String getChannelType() {
            return "direct";
        }

        @Override
        public Version getVersion() {
            return localNode.getVersion();
        }
    }

    /**
     * Returns the internal thread pool
     */
    public ThreadPool getThreadPool() {
        return threadPool;
    }

    private boolean isLocalNode(DiscoveryNode discoveryNode) {
        return Objects.requireNonNull(discoveryNode, "discovery node must not be null").equals(localNode);
    }
}
