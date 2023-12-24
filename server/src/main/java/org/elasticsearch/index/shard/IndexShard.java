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

package org.elasticsearch.index.shard;

import com.carrotsearch.hppc.ObjectLongMap;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.UsageTrackingQueryCachingPolicy;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.ThreadInterruptedException;
import org.elasticsearch.Assertions;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.upgrade.post.UpgradeRequest;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RecoverySource.SnapshotRecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.AsyncIOProcessor;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.index.cache.bitset.ShardBitsetFilterCache;
import org.elasticsearch.index.cache.request.ShardRequestCache;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.engine.CommitStats;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.engine.InternalEngineFactory;
import org.elasticsearch.index.engine.RefreshFailedEngineException;
import org.elasticsearch.index.engine.Segment;
import org.elasticsearch.index.engine.SegmentsStats;
import org.elasticsearch.index.fielddata.FieldDataStats;
import org.elasticsearch.index.fielddata.ShardFieldData;
import org.elasticsearch.index.flush.FlushStats;
import org.elasticsearch.index.get.GetStats;
import org.elasticsearch.index.get.ShardGetService;
import org.elasticsearch.index.mapper.DocumentMapperForType;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.UidFieldMapper;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.recovery.RecoveryStats;
import org.elasticsearch.index.refresh.RefreshStats;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.search.stats.ShardSearchStats;
import org.elasticsearch.index.seqno.ReplicationTracker;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.PrimaryReplicaSyncer.ResyncTask;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.Store.MetadataSnapshot;
import org.elasticsearch.index.store.StoreFileMetaData;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogConfig;
import org.elasticsearch.index.translog.TranslogStats;
import org.elasticsearch.index.warmer.ShardIndexWarmerService;
import org.elasticsearch.index.warmer.WarmerStats;
import org.elasticsearch.indices.IndexingMemoryController;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.TypeMissingException;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.cluster.IndicesClusterStateService;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.indices.recovery.RecoveryFailedException;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.suggest.completion.CompletionFieldStats;
import org.elasticsearch.search.suggest.completion.CompletionStats;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.channels.ClosedByInterruptException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.elasticsearch.index.mapper.SourceToParse.source;
// NOTE: htt, shard的操作入口，包括shard的创建（包括shard的数据恢复），shard状态的调整；数据写入、删除、get、以及searcher提供；引擎获得；shard的flush()和refresh(), translog的roll(); shard的段合并处理；shard的globalCheckpoint和localCheckpoint()处理
public class IndexShard extends AbstractIndexShardComponent implements IndicesClusterStateService.Shard {

    private final ThreadPool threadPool;
    private final MapperService mapperService;  // NOTE: htt, 更新字段映射，并且会进行映射检查
    private final IndexCache indexCache;  // NOTE: htt, 索引的查询缓存
    private final Store store;  // NOTE: htt, 用于访问shard，每个shard有对应实例，用于获取shard下的段文件的元信息，以及各个段文件的checksum等，并对段文件等进行校验
    private final InternalIndexingStats internalIndexingStats;  // NOTE: htt, 记录索引操作的统计信息，包括index/delete/noop等
    private final ShardSearchStats searchStats = new ShardSearchStats();  // NOTE: htt, shard的查询统计
    private final ShardGetService getService; // NOTE: htt, shard级别根据 get 对应的id获取内容
    private final ShardIndexWarmerService shardWarmerService;  // NOTE: htt, shardIndex 的 warmer统计，包括总的个数以时间
    private final ShardRequestCache requestCacheStats;  // htt: htt, shard 请求的cache情况
    private final ShardFieldData shardFieldData; // NOTE: htt, shard下字段的cache统计信息
    private final ShardBitsetFilterCache shardBitsetFilterCache;  // NOTE: htt, shard 的 bitsetFilter 的cache大小
    private final Object mutex = new Object();
    private final String checkIndexOnStartup; // NOTE: htt, 索引shard启动时对shard进行检查，默认为false
    private final CodecService codecService;  // NOTE: htt, lucene文件编解码的Codec
    private final Engine.Warmer warmer;  // NOTE: htt, get new searcher to warm new segments
    private final SimilarityService similarityService;  // NOTE: htt, 相似度服务，提供默认为BM25的相似度评分
    private final TranslogConfig translogConfig;  // NOTE: htt, translog config including indexSettings/shardId/translogPath
    private final IndexEventListener indexEventListener;  // NOTE: htt, index shard相关事件改变之后的处理
    private final QueryCachingPolicy cachingPolicy; // NOTE: htt, query cache 策略
    private final Supplier<Sort> indexSortSupplier; // NOTE: htt, index sort
    // Package visible for testing
    final CircuitBreakerService circuitBreakerService;  // NOTE: htt, CircuitBreaker Service including register breaker

    private final SearchOperationListener searchOperationListener; // NOTE: htt, 搜索场景下的 operation listener

    private final ReplicationTracker replicationTracker; // NOTE: htt, 当前shard所有副本的checkPoint（本地以及全局）记录和更新机制，并记录待同步的allocationID

    protected volatile ShardRouting shardRouting; // NOTE: htt, 当前具体的shard，并且具体在哪台机器，shard routing info include shardId, primary, unassigendInfo, recoverySource, allocationId
    protected volatile IndexShardState state; // NOTE: htt, index shard 处于的状态
    protected volatile long primaryTerm; // NOTE: htt, 当前shard的primary term，该值在新的shard选为主后递增
    protected final AtomicReference<Engine> currentEngineReference = new AtomicReference<>(); // NOTE: htt, 当前的引擎，默认为 InternalEngine
    protected final EngineFactory engineFactory; // NOTE: htt, engine factory

    private final IndexingOperationListener indexingOperationListeners;  // NOTE: htt, indexing operatation listener including index pre/post, delete pre/post
    private final Runnable globalCheckpointSyncer;

    Runnable getGlobalCheckpointSyncer() {
        return globalCheckpointSyncer;
    }

    @Nullable
    private RecoveryState recoveryState;  // NOTE: htt, recovery state including recovery type, index/check index/translog step info

    private final RecoveryStats recoveryStats = new RecoveryStats();  // NOTE: htt, recovery shard恢复统计
    private final MeanMetric refreshMetric = new MeanMetric(); // NOTE: htt, refresh刷新统计
    private final MeanMetric flushMetric = new MeanMetric(); // NOTE; htt, flush统计
    private final CounterMetric periodicFlushMetric = new CounterMetric(); // NOTE: htt, 定期flush统计

    private final ShardEventListener shardEventListener = new ShardEventListener(); // NOTE: htt, shard 异常事件监听

    private final ShardPath path; // NOTE: htt, 包括 ShardId信息以及shard的路径信息，并包含选出新的shard路径

    private final IndexShardOperationPermits indexShardOperationPermits;  // NOTE: htt, 允许阻塞（同步以及异步）一段时间，并接下来执行 onBlocked.run()操作

    private static final EnumSet<IndexShardState> readAllowedStates = EnumSet.of(IndexShardState.STARTED, IndexShardState.POST_RECOVERY); // NOTE: htt, start/post_recovery状态允许进行数据读取
    // for primaries, we only allow to write when actually started (so the cluster has decided we started)
    // in case we have a relocation of a primary, we also allow to write after phase 2 completed, where the shard may be
    // in state RECOVERING or POST_RECOVERY.
    // for replicas, replication is also allowed while recovering, since we index also during recovery to replicas and rely on version checks to make sure its consistent
    // a relocated shard can also be target of a replication if the relocation target has not been marked as active yet and is syncing it's changes back to the relocation source
    private static final EnumSet<IndexShardState> writeAllowedStates = EnumSet.of(IndexShardState.RECOVERING, IndexShardState.POST_RECOVERY, IndexShardState.STARTED); // NOTE: htt, started/recovering/post_recovery 状态允许执行写操作

    private final IndexSearcherWrapper searcherWrapper;  // NOTE: htt, index searcher 查询包装，包括 directoryReader/indexSearcher

    /**
     * True if this shard is still indexing (recently) and false if we've been idle for long enough (as periodically checked by {@link
     * IndexingMemoryController}).
     */
    private final AtomicBoolean active = new AtomicBoolean(); // NOTE: htt, 当前shard是否处于active状态
    /**
     * Allows for the registration of listeners that are called when a change becomes visible for search.
     */
    private final RefreshListeners refreshListeners;  // NOTE: htt, refresh监听器，处理translog监听以及段的refresh

    public IndexShard(
            ShardRouting shardRouting,
            IndexSettings indexSettings,
            ShardPath path,
            Store store,
            Supplier<Sort> indexSortSupplier,
            IndexCache indexCache,
            MapperService mapperService,
            SimilarityService similarityService,
            @Nullable EngineFactory engineFactory,
            IndexEventListener indexEventListener,
            IndexSearcherWrapper indexSearcherWrapper,
            ThreadPool threadPool,
            BigArrays bigArrays,
            Engine.Warmer warmer,
            List<SearchOperationListener> searchOperationListener,
            List<IndexingOperationListener> listeners,
            Runnable globalCheckpointSyncer,
            CircuitBreakerService circuitBreakerService) throws IOException {
        super(shardRouting.shardId(), indexSettings);
        assert shardRouting.initializing();
        this.shardRouting = shardRouting; // NOTE:htt, 暂时没有大的调整
        final Settings settings = indexSettings.getSettings();
        this.codecService = new CodecService(mapperService, logger);
        this.warmer = warmer;
        this.similarityService = similarityService;
        Objects.requireNonNull(store, "Store must be provided to the index shard");
        this.engineFactory = engineFactory == null ? new InternalEngineFactory() : engineFactory;
        this.store = store;
        this.indexSortSupplier = indexSortSupplier;
        this.indexEventListener = indexEventListener;
        this.threadPool = threadPool;
        this.mapperService = mapperService;
        this.indexCache = indexCache;
        this.internalIndexingStats = new InternalIndexingStats();
        final List<IndexingOperationListener> listenersList = new ArrayList<>(listeners);
        listenersList.add(internalIndexingStats);
        this.indexingOperationListeners = new IndexingOperationListener.CompositeListener(listenersList, logger); // NOTE: composite listener of indexing operation listener
        this.globalCheckpointSyncer = globalCheckpointSyncer;
        final List<SearchOperationListener> searchListenersList = new ArrayList<>(searchOperationListener);
        searchListenersList.add(searchStats);
        this.searchOperationListener = new SearchOperationListener.CompositeListener(searchListenersList, logger);  // NOTE: htt, 组合的 searchOpeartion listener
        this.getService = new ShardGetService(indexSettings, this, mapperService); // NOTE: htt, shard级别根据 get 对应的id获取内容
        this.shardWarmerService = new ShardIndexWarmerService(shardId, indexSettings);
        this.requestCacheStats = new ShardRequestCache();
        this.shardFieldData = new ShardFieldData(); // NOTE: htt, shard下字段的cache统计信息
        this.shardBitsetFilterCache = new ShardBitsetFilterCache(shardId, indexSettings);  // NOTE: htt, shard 的 bitsetFilter 的cache大小
        state = IndexShardState.CREATED; // NOTE: htt, 首次为创建状态
        this.path = path;
        this.circuitBreakerService = circuitBreakerService;
        /* create engine config */
        logger.debug("state: [CREATED]");

        this.checkIndexOnStartup = indexSettings.getValue(IndexSettings.INDEX_CHECK_ON_STARTUP); // NOTE: htt, 默认检查为关
        this.translogConfig = new TranslogConfig(shardId, shardPath().resolveTranslog(), indexSettings, bigArrays); // NOTE: htt, translog配置
        this.replicationTracker = new ReplicationTracker(shardId, shardRouting.allocationId().getId(), indexSettings,
            SequenceNumbers.UNASSIGNED_SEQ_NO); // NOTE: htt, 创建恢复分片机制
        // the query cache is a node-level thing, however we want the most popular filters
        // to be computed on a per-shard basis
        if (IndexModule.INDEX_QUERY_CACHE_EVERYTHING_SETTING.get(settings)) { // NOTE: htt, 默认是不缓存，即采用 UsageTrackingQueryCachingPolicy
            cachingPolicy = QueryCachingPolicy.ALWAYS_CACHE; // NOTE: htt, 总是对查询缓存
        } else {
            cachingPolicy = new UsageTrackingQueryCachingPolicy();  // NOTE: htt, 记录查询的频率，然后根据频率值判断是否缓存query查询（耗时查询频限2次，一般查询5次则进行缓存）
        }
        indexShardOperationPermits = new IndexShardOperationPermits(shardId, logger, threadPool); // NOTE: htt, 允许阻塞（同步以及异步）一段时间，并接下来执行 onBlocked.run()操作
        searcherWrapper = indexSearcherWrapper;
        primaryTerm = indexSettings.getIndexMetaData().primaryTerm(shardId.id());
        refreshListeners = buildRefreshListeners(); // NOTE: htt, 构建 refresh监听
        persistMetadata(path, indexSettings, shardRouting, null, logger); // NOTE: htt, 记录shard的元信息到${shard_path}/_state/state-xx.st文件中
    }

    public ThreadPool getThreadPool() {
        return this.threadPool;
    }

    public Store store() {
        return this.store;
    }

    /**
     * Return the sort order of this index, or null if the index has no sort.
     */
    public Sort getIndexSort() {
        return indexSortSupplier.get();
    }

    public ShardGetService getService() {
        return this.getService;
    }

    public ShardBitsetFilterCache shardBitsetFilterCache() {
        return shardBitsetFilterCache;
    }

    public MapperService mapperService() {
        return mapperService;
    }

    public SearchOperationListener getSearchOperationListener() {
        return this.searchOperationListener;
    }

    public ShardIndexWarmerService warmerService() {
        return this.shardWarmerService;
    }

    public ShardRequestCache requestCache() {
        return this.requestCacheStats;
    }

    public ShardFieldData fieldData() {
        return this.shardFieldData;
    }

    /**
     * Returns the primary term the index shard is on. See {@link org.elasticsearch.cluster.metadata.IndexMetaData#primaryTerm(int)}
     */
    public long getPrimaryTerm() {
        return this.primaryTerm;
    }

    /**
     * Returns the latest cluster routing entry received with this shard.
     */
    @Override
    public ShardRouting routingEntry() {
        return this.shardRouting;
    }

    public QueryCachingPolicy getQueryCachingPolicy() {
        return cachingPolicy;
    }


    @Override
    public void updateShardState(final ShardRouting newRouting,
                                 final long newPrimaryTerm,
                                 final BiConsumer<IndexShard, ActionListener<ResyncTask>> primaryReplicaSyncer,
                                 final long applyingClusterStateVersion,
                                 final Set<String> inSyncAllocationIds,
                                 final IndexShardRoutingTable routingTable,
                                 final Set<String> pre60AllocationIds) throws IOException {
        final ShardRouting currentRouting;
        synchronized (mutex) { // NOTE: htt, 加锁处理
            currentRouting = this.shardRouting;

            if (!newRouting.shardId().equals(shardId())) {
                throw new IllegalArgumentException("Trying to set a routing entry with shardId " + newRouting.shardId() + " on a shard with shardId " + shardId());
            }
            if ((currentRouting == null || newRouting.isSameAllocation(currentRouting)) == false) {
                throw new IllegalArgumentException("Trying to set a routing entry with a different allocation. Current " + currentRouting + ", new " + newRouting);
            }
            if (currentRouting != null && currentRouting.primary() && newRouting.primary() == false) {
                throw new IllegalArgumentException("illegal state: trying to move shard from primary mode to replica mode. Current "
                    + currentRouting + ", new " + newRouting);
            }

            if (newRouting.primary()) { // NOTE: htt, 主shard进行更新
                replicationTracker.updateFromMaster(applyingClusterStateVersion, inSyncAllocationIds, routingTable, pre60AllocationIds); // NOTE: htt, 更新allocation下checkPoints信息
            }

            if (state == IndexShardState.POST_RECOVERY && newRouting.active()) { // NOTE: htt, 如果已经恢复完毕，并且处于激活状态，则将状态调整为started
                assert currentRouting.active() == false : "we are in POST_RECOVERY, but our shard routing is active " + currentRouting;

                assert currentRouting.isRelocationTarget() == false || currentRouting.primary() == false ||
                    recoveryState.getSourceNode().getVersion().before(Version.V_6_0_0_alpha1) ||
                        replicationTracker.isPrimaryMode() :
                    "a primary relocation is completed by the master, but primary mode is not active " + currentRouting;

                changeState(IndexShardState.STARTED, "global state is [" + newRouting.state() + "]"); // NOTE: htt, 状态调整到started
            } else if (currentRouting.primary() && currentRouting.relocating() && replicationTracker.isPrimaryMode() == false &&
                (newRouting.relocating() == false || newRouting.equalsIgnoringMetaData(currentRouting) == false)) {
                // if the shard is not in primary mode anymore (after primary relocation) we have to fail when any changes in shard routing occur (e.g. due to recovery
                // failure / cancellation). The reason is that at the moment we cannot safely reactivate primary mode without risking two
                // active primaries.
                throw new IndexShardRelocatedException(shardId(), "Shard is marked as relocated, cannot safely move to state " + newRouting.state());
            }
            assert newRouting.active() == false || state == IndexShardState.STARTED || state == IndexShardState.CLOSED :
                "routing is active, but local shard state isn't. routing: " + newRouting + ", local state: " + state;
            persistMetadata(path, indexSettings, newRouting, currentRouting, logger);  // NOTE: htt, 持久化index shard的元信息
            final CountDownLatch shardStateUpdated = new CountDownLatch(1);

            if (newRouting.primary()) { // NOTE: htt, 新routing下该shard为主
                if (newPrimaryTerm == primaryTerm) { // NOTE: htt, 是当前primaryTerm周期情况下
                    if (currentRouting.initializing() && newRouting.active()) {
                        if (currentRouting.isRelocationTarget() == false) {
                            // the master started a recovering primary, activate primary mode.
                            replicationTracker.activatePrimaryMode(getLocalCheckpoint()); // NOTE: htt, 获取primary shard，并激活主mode
                        } else if (recoveryState.getSourceNode().getVersion().before(Version.V_6_0_0_alpha1)) {
                            // there was no primary context hand-off in < 6.0.0, need to manually activate the shard
                            replicationTracker.activatePrimaryMode(getLocalCheckpoint());
                            // Flush the translog as it may contain operations with no sequence numbers. We want to make sure those
                            // operations will never be replayed as part of peer recovery to avoid an arbitrary mixture of operations with
                            // seq# (due to active indexing) and operations without a seq# coming from the translog. We therefore flush
                            // to create a lucene commit point to an empty translog file.
                            getEngine().flush(false, true);
                        }
                    }
                } else { // NOTE: htt, 新routing下该shard为主，但是newPrimaryTerm 并不和当前的 primaryTerm相同，并且必须必当前 primaryTerm 大
                    assert currentRouting.primary() == false : "term is only increased as part of primary promotion";
                    /* Note that due to cluster state batching an initializing primary shard term can failed and re-assigned
                     * in one state causing it's term to be incremented. Note that if both current shard state and new
                     * shard state are initializing, we could replace the current shard and reinitialize it. It is however
                     * possible that this shard is being started. This can happen if:
                     * 1) Shard is post recovery and sends shard started to the master
                     * 2) Node gets disconnected and rejoins
                     * 3) Master assigns the shard back to the node
                     * 4) Master processes the shard started and starts the shard
                     * 5) The node process the cluster state where the shard is both started and primary term is incremented.
                     *
                     * We could fail the shard in that case, but this will cause it to be removed from the insync allocations list
                     * potentially preventing re-allocation.
                     */
                    assert newRouting.initializing() == false :
                        "a started primary shard should never update its term; "
                            + "shard " + newRouting + ", "
                            + "current term [" + primaryTerm + "], "
                            + "new term [" + newPrimaryTerm + "]";
                    assert newPrimaryTerm > primaryTerm :
                        "primary terms can only go up; current term [" + primaryTerm + "], new term [" + newPrimaryTerm + "]";
                    /*
                     * Before this call returns, we are guaranteed that all future operations are delayed and so this happens before we
                     * increment the primary term. The latch is needed to ensure that we do not unblock operations before the primary term is
                     * incremented.
                     */
                    // to prevent primary relocation handoff while resync is not completed
                    boolean resyncStarted = primaryReplicaResyncInProgress.compareAndSet(false, true); // NOTE: htt, 判断当前是否在处理中
                    if (resyncStarted == false) {
                        throw new IllegalStateException("cannot start resync while it's already in progress");
                    }
                    indexShardOperationPermits.asyncBlockOperations( // NOTE: htt, 异步执行
                        30,
                        TimeUnit.MINUTES, // NOTE: htt, 阻塞30min
                        () -> {
                            shardStateUpdated.await(); // NOTE: htt, 等待
                            try {
                                /*
                                 * If this shard was serving as a replica shard when another shard was promoted to primary then the state of
                                 * its local checkpoint tracker was reset during the primary term transition. In particular, the local
                                 * checkpoint on this shard was thrown back to the global checkpoint and the state of the local checkpoint
                                 * tracker above the local checkpoint was destroyed. If the other shard that was promoted to primary
                                 * subsequently fails before the primary/replica re-sync completes successfully and we are now being
                                 * promoted, the local checkpoint tracker here could be left in a state where it would re-issue sequence
                                 * numbers. To ensure that this is not the case, we restore the state of the local checkpoint tracker by
                                 * replaying the translog and marking any operations there are completed.
                                 */
                                final Engine engine = getEngine();
                                engine.restoreLocalCheckpointFromTranslog(); // NOTE: htt, 从本地translogSnapShot中恢复localCheckPoint的位置等数据
                                if (indexSettings.getIndexVersionCreated().onOrBefore(Version.V_6_0_0_alpha1)) {
                                    // an index that was created before sequence numbers were introduced may contain operations in its
                                    // translog that do not have a sequence numbers. We want to make sure those operations will never
                                    // be replayed as part of peer recovery to avoid an arbitrary mixture of operations with seq# (due
                                    // to active indexing) and operations without a seq# coming from the translog. We therefore flush
                                    // to create a lucene commit point to an empty translog file.
                                    engine.flush(false, true);
                                }
                                /* Rolling the translog generation is not strictly needed here (as we will never have collisions between
                                 * sequence numbers in a translog generation in a new primary as it takes the last known sequence number
                                 * as a starting point), but it simplifies reasoning about the relationship between primary terms and
                                 * translog generations.
                                 */
                                engine.rollTranslogGeneration(); // NOTE: htt, translog关闭当前 translog-xx.tlog，并递增generation，然后创建新的 translog-xx.tlog
                                engine.fillSeqNoGaps(newPrimaryTerm);  // NOTE: htt, 填补localCheckPoint和localCheckPoint记录的maxSeqNo之间空缺的操作日志，以NoOp形式
                                replicationTracker.updateLocalCheckpoint(currentRouting.allocationId().getId(),
                                    getEngine().getLocalCheckpointTracker().getCheckpoint()); // NOTE: htt, 更新本地的localCheckPoint
                                primaryReplicaSyncer.accept(this, new ActionListener<ResyncTask>() {
                                    @Override
                                    public void onResponse(ResyncTask resyncTask) {
                                        logger.info("primary-replica resync completed with {} operations",
                                            resyncTask.getResyncedOperations());
                                        boolean resyncCompleted = primaryReplicaResyncInProgress.compareAndSet(true, false); // NOTE: htt, 同步处理完成将 进行中状态设置为false
                                        assert resyncCompleted : "primary-replica resync finished but was not started";
                                    }

                                    @Override
                                    public void onFailure(Exception e) {
                                        boolean resyncCompleted = primaryReplicaResyncInProgress.compareAndSet(true, false); // NOTE: htt, 同步处理异常将 进行中状态设置为false
                                        assert resyncCompleted : "primary-replica resync finished but was not started";
                                        if (state == IndexShardState.CLOSED) {
                                            // ignore, shutting down
                                        } else {
                                            failShard("exception during primary-replica resync", e);  // NOTE: htt, shard执行异常
                                        }
                                    }
                                });
                            } catch (final AlreadyClosedException e) {
                                // okay, the index was deleted
                            }
                        },
                        e -> failShard("exception during primary term transition", e));
                    replicationTracker.activatePrimaryMode(getEngine().getLocalCheckpointTracker().getCheckpoint()); // NOTE: htt, 激活primaryMode，注意这里可能比 asyncBlockOperations.onBlocked()操作先执行
                    primaryTerm = newPrimaryTerm; // NOTE: htt, 设置新的primaryTerm
                }
            }
            // set this last, once we finished updating all internal state.
            this.shardRouting = newRouting;

            assert this.shardRouting.primary() == false ||
                this.shardRouting.started() == false || // note that we use started and not active to avoid relocating shards
                this.replicationTracker.isPrimaryMode()
                : "an started primary must be in primary mode " + this.shardRouting;
            shardStateUpdated.countDown(); // NOTE: htt, 释放锁，这样 asyncBlockOperations 中的onBlocked()异步操作可以进一步继续
        }
        if (currentRouting != null && currentRouting.active() == false && newRouting.active()) {
            indexEventListener.afterIndexShardStarted(this); // NOTE: htt, shard启动后监听
        }
        if (newRouting.equals(currentRouting) == false) {
            indexEventListener.shardRoutingChanged(this, currentRouting, newRouting); // NOTE: htt, shard改变后检查处理
        }
    }

    /**
     * Marks the shard as recovering based on a recovery state, fails with exception is recovering is not allowed to be set.
     */
    public IndexShardState markAsRecovering(String reason, RecoveryState recoveryState) throws IndexShardStartedException,
        IndexShardRelocatedException, IndexShardRecoveringException, IndexShardClosedException {
        synchronized (mutex) {
            if (state == IndexShardState.CLOSED) {
                throw new IndexShardClosedException(shardId);
            }
            if (state == IndexShardState.STARTED) {
                throw new IndexShardStartedException(shardId);
            }
            if (state == IndexShardState.RECOVERING) {
                throw new IndexShardRecoveringException(shardId);
            }
            if (state == IndexShardState.POST_RECOVERY) {
                throw new IndexShardRecoveringException(shardId);
            }
            this.recoveryState = recoveryState;
            return changeState(IndexShardState.RECOVERING, reason); // NOTE: htt, 设置shard的状态为RECOVERING
        }
    }

    private final AtomicBoolean primaryReplicaResyncInProgress = new AtomicBoolean(); // NOTE: htt, 主备重新同步处理中

    /**
     * Completes the relocation. Operations are blocked and current operations are drained before changing state to relocated. The provided
     * {@link Runnable} is executed after all operations are successfully blocked.
     *
     * @param consumer a {@link Runnable} that is executed after operations are blocked
     * @throws IllegalIndexShardStateException if the shard is not relocating due to concurrent cancellation
     * @throws InterruptedException            if blocking operations is interrupted
     */
    public void relocated(final Consumer<ReplicationTracker.PrimaryContext> consumer) throws IllegalIndexShardStateException, InterruptedException {
        assert shardRouting.primary() : "only primaries can be marked as relocated: " + shardRouting;
        try {
            indexShardOperationPermits.blockOperations(30, TimeUnit.MINUTES, () -> { // NOTE: htt, 最大阻塞30min,指定期间，先delay，执行过程中，如果有新的请求进来则放入队列，待执行完毕后，再将队列请求重放
                // no shard operation permits are being held here, move state from started to relocated
                assert indexShardOperationPermits.getActiveOperationsCount() == 0 :
                        "in-flight operations in progress while moving shard state to relocated";
                /*
                 * We should not invoke the runnable under the mutex as the expected implementation is to handoff the primary context via a
                 * network operation. Doing this under the mutex can implicitly block the cluster state update thread on network operations.
                 */
                verifyRelocatingState();
                final ReplicationTracker.PrimaryContext primaryContext = replicationTracker.startRelocationHandoff(); // NOTE: htt, 当前主shard的内容
                try {
                    consumer.accept(primaryContext); // NOTE: htt, 处理primaryContext，将内容发送给新的主shard，并收到回包，此处会一直等待回包
                    synchronized (mutex) {
                        verifyRelocatingState();
                        replicationTracker.completeRelocationHandoff(); // make changes to primaryMode flag only under mutex // NOTE: htt, 将shard primaryMode设为false
                    }
                } catch (final Exception e) {
                    try {
                        replicationTracker.abortRelocationHandoff();
                    } catch (final Exception inner) {
                        e.addSuppressed(inner);
                    }
                    throw e;
                }
            });
        } catch (TimeoutException e) {
            logger.warn("timed out waiting for relocation hand-off to complete");
            // This is really bad as ongoing replication operations are preventing this shard from completing relocation hand-off.
            // Fail primary relocation source and target shards.
            failShard("timed out waiting for relocation hand-off to complete", null);
            throw new IndexShardClosedException(shardId(), "timed out waiting for relocation hand-off to complete");
        }
    }

    private void verifyRelocatingState() {
        if (state != IndexShardState.STARTED) { // NOTE: htt, 状态必须为started
            throw new IndexShardNotStartedException(shardId, state);
        }
        /*
         * If the master cancelled recovery, the target will be removed and the recovery will be cancelled. However, it is still possible
         * that we concurrently end up here and therefore have to protect that we do not mark the shard as relocated when its shard routing
         * says otherwise.
         */

        if (shardRouting.relocating() == false) { // NOTE: htt, relocate必须为true
            throw new IllegalIndexShardStateException(shardId, IndexShardState.STARTED,
                ": shard is no longer relocating " + shardRouting);
        }

        if (primaryReplicaResyncInProgress.get()) { // NOTE: htt, 主备同步在进行中则出错
            throw new IllegalIndexShardStateException(shardId, IndexShardState.STARTED,
                ": primary relocation is forbidden while primary-replica resync is in progress " + shardRouting);
        }
    }

    @Override
    public IndexShardState state() {
        return state;
    }

    /**
     * Changes the state of the current shard
     *
     * @param newState the new shard state
     * @param reason   the reason for the state change
     * @return the previous shard state
     */
    private IndexShardState changeState(IndexShardState newState, String reason) {
        assert Thread.holdsLock(mutex);
        logger.debug("state: [{}]->[{}], reason [{}]", state, newState, reason);
        IndexShardState previousState = state;
        state = newState;
        this.indexEventListener.indexShardStateChanged(this, previousState, newState, reason); // NOTE: htt, 监听indexShard状态变化
        return previousState;
    }

    public Engine.IndexResult applyIndexOperationOnPrimary(long version, VersionType versionType, SourceToParse sourceToParse,
                                                           long autoGeneratedTimestamp, boolean isRetry) throws IOException {
        return applyIndexOperation(SequenceNumbers.UNASSIGNED_SEQ_NO, primaryTerm, version, versionType, autoGeneratedTimestamp,
            isRetry, Engine.Operation.Origin.PRIMARY, sourceToParse); // NOTE: htt, 如果有mapping则先返回更新mapping，并在master执行； 否则可以写入index
    }

    public Engine.IndexResult applyIndexOperationOnReplica(long seqNo, long version, VersionType versionType,
                                                           long autoGeneratedTimeStamp, boolean isRetry, SourceToParse sourceToParse)
        throws IOException { // NOTE: htt, 备shard上执行index写入操作
        return applyIndexOperation(seqNo, primaryTerm, version, versionType, autoGeneratedTimeStamp, isRetry,
            Engine.Operation.Origin.REPLICA, sourceToParse); // NOTE: htt, 处理备shard上的index操作
    }

    private Engine.IndexResult applyIndexOperation(long seqNo, long opPrimaryTerm, long version, VersionType versionType,
                                                   long autoGeneratedTimeStamp, boolean isRetry, Engine.Operation.Origin origin,
                                                   SourceToParse sourceToParse) throws IOException { // NOTE: htt, 如果有mapping则先返回更新mapping，并在master执行； 否则可以写入index
        assert opPrimaryTerm <= this.primaryTerm : "op term [ " + opPrimaryTerm + " ] > shard term [" + this.primaryTerm + "]";
        assert versionType.validateVersionForWrites(version);
        ensureWriteAllowed(origin); // NOTE: htt, 验证允许写
        Engine.Index operation;
        try {
            operation = prepareIndex(docMapper(sourceToParse.type()), indexSettings.getIndexVersionCreated(), sourceToParse, seqNo,
                    opPrimaryTerm, version, versionType, origin,
                autoGeneratedTimeStamp, isRetry); // NOTE: htt, 准备index写入操作
            Mapping update = operation.parsedDoc().dynamicMappingsUpdate(); // NOTE: htt, 获取更新mapping
            if (update != null) {
                return new Engine.IndexResult(update); // NOTE: htt, 如果需要更新mapping映射，则返回更新的mapping，并在master上执行; 否则继续写入
            }
        } catch (Exception e) {
            // We treat any exception during parsing and or mapping update as a document level failure
            // with the exception side effects of closing the shard. Since we don't have the shard, we
            // can not raise an exception that may block any replication of previous operations to the
            // replicas
            verifyNotClosed(e);
            return new Engine.IndexResult(e, version, seqNo);
        }

        return index(getEngine(), operation); // NOTE: htt, 执行lucene和translog写入
    }

    public static Engine.Index prepareIndex(DocumentMapperForType docMapper, Version indexCreatedVersion, SourceToParse source, long seqNo,
            long primaryTerm, long version, VersionType versionType, Engine.Operation.Origin origin, long autoGeneratedIdTimestamp,
            boolean isRetry) {
        long startTime = System.nanoTime();
        ParsedDocument doc = docMapper.getDocumentMapper().parse(source); // NOTE: htt, 使用映射 解析_source内容
        if (docMapper.getMapping() != null) { // NOTE: htt, 如果有新的 mapping
            doc.addDynamicMappingsUpdate(docMapper.getMapping()); // NOTE: htt, 添加新的mapping
        }
        Term uid;
        if (indexCreatedVersion.onOrAfter(Version.V_6_0_0_beta1)) {
            uid = new Term(IdFieldMapper.NAME, Uid.encodeId(doc.id())); // NOTE: htt, 构造 {_id, Uid.encodeId(doc.id) } 的term
        } else if (docMapper.getDocumentMapper().idFieldMapper().fieldType().indexOptions() != IndexOptions.NONE) {
            uid = new Term(IdFieldMapper.NAME, doc.id());
        } else {
            uid = new Term(UidFieldMapper.NAME, Uid.createUidAsBytes(doc.type(), doc.id()));
        }
        return new Engine.Index(uid, doc, seqNo, primaryTerm, version, versionType, origin, startTime, autoGeneratedIdTimestamp, isRetry);
    }

    private Engine.IndexResult index(Engine engine, Engine.Index index) throws IOException {
        active.set(true);
        final Engine.IndexResult result;
        index = indexingOperationListeners.preIndex(shardId, index); // NOTE: htt, index之前处理
        try {
            if (logger.isTraceEnabled()) {
                // don't use index.source().utf8ToString() here source might not be valid UTF-8
                logger.trace("index [{}][{}] (seq# [{}])",  index.type(), index.id(), index.seqNo());
            }
            result = engine.index(index); // NOTE: htt, 引擎执行写入操作
        } catch (Exception e) {
            indexingOperationListeners.postIndex(shardId, index, e); // NOTE: htt, index之后处理
            throw e;
        }
        indexingOperationListeners.postIndex(shardId, index, result); // NOTE: htt, index之后处理
        return result;
    }

    public Engine.NoOpResult markSeqNoAsNoop(long seqNo, String reason) throws IOException { // NOTE: htt, 将_seq_no记录一条 NoOp操作，然后记录到translog
        return markSeqNoAsNoop(seqNo, primaryTerm, reason, Engine.Operation.Origin.REPLICA);
    }

    private Engine.NoOpResult markSeqNoAsNoop(long seqNo, long opPrimaryTerm, String reason,
                                              Engine.Operation.Origin origin) throws IOException {
        assert opPrimaryTerm <= this.primaryTerm : "op term [ " + opPrimaryTerm + " ] > shard term [" + this.primaryTerm + "]";
        long startTime = System.nanoTime();
        ensureWriteAllowed(origin);
        final Engine.NoOp noOp = new Engine.NoOp(seqNo, opPrimaryTerm, origin, startTime, reason);
        return noOp(getEngine(), noOp);
    }

    private Engine.NoOpResult noOp(Engine engine, Engine.NoOp noOp) {
        active.set(true);
        if (logger.isTraceEnabled()) {
            logger.trace("noop (seq# [{}])", noOp.seqNo());
        }
        return engine.noOp(noOp); // NOTE: htt, 记录noop操作
    }

    public Engine.DeleteResult applyDeleteOperationOnPrimary(long version, String type, String id, VersionType versionType)
        throws IOException {
        return applyDeleteOperation(SequenceNumbers.UNASSIGNED_SEQ_NO, primaryTerm, version, type, id, versionType,
            Engine.Operation.Origin.PRIMARY); // NOTE: htt, 主shard上进行删除操作
    }

    public Engine.DeleteResult applyDeleteOperationOnReplica(long seqNo, long version, String type, String id,
                                                             VersionType versionType) throws IOException {
        return applyDeleteOperation(seqNo, primaryTerm, version, type, id, versionType, Engine.Operation.Origin.REPLICA); // NOTE: htt, 被shard进行删除操作
    }

    private Engine.DeleteResult applyDeleteOperation(long seqNo, long opPrimaryTerm, long version, String type, String id,
                                                     VersionType versionType, Engine.Operation.Origin origin) throws IOException { // NOTE: htt, 执行删除操作，如果有mapping更新则先进行返回mapping；否则执行本地删除
        assert opPrimaryTerm <= this.primaryTerm : "op term [ " + opPrimaryTerm + " ] > shard term [" + this.primaryTerm + "]";
        assert versionType.validateVersionForWrites(version);
        ensureWriteAllowed(origin);
        if (indexSettings().isSingleType()) { // NOTE: htt, 单一 type
            // When there is a single type, the unique identifier is only composed of the _id,
            // so there is no way to differenciate foo#1 from bar#1. This is especially an issue
            // if a user first deletes foo#1 and then indexes bar#1: since we do not encode the
            // _type in the uid it might look like we are reindexing the same document, which
            // would fail if bar#1 is indexed with a lower version than foo#1 was deleted with.
            // In order to work around this issue, we make deletions create types. This way, we
            // fail if index and delete operations do not use the same type.
            try{
                Mapping update = docMapper(type).getMapping();
                if (update != null) {
                    return new Engine.DeleteResult(update); // NOTE: htt, 删除可能带来mapping更新，则返回
                }
            } catch (MapperParsingException | IllegalArgumentException | TypeMissingException e) {
                return new Engine.DeleteResult(e, version, seqNo, false);
            }
        }
        final Term uid = extractUidForDelete(type, id); // NOTE: htt, 当前id主键
        final Engine.Delete delete = prepareDelete(type, id, uid, seqNo, opPrimaryTerm, version,
            versionType, origin); // NOTE: htt, 删除请求
        return delete(getEngine(), delete); // NOTE: htt, 执行删除操作
    }

    private static Engine.Delete prepareDelete(String type, String id, Term uid, long seqNo, long primaryTerm, long version,
                                               VersionType versionType, Engine.Operation.Origin origin) {
        long startTime = System.nanoTime();
        return new Engine.Delete(type, id, uid, seqNo, primaryTerm, version, versionType, origin, startTime); // NOTE: htt, 构造删除请求
    }

    private Term extractUidForDelete(String type, String id) {
        if (indexSettings.getIndexVersionCreated().onOrAfter(Version.V_6_0_0_beta1)) {
            assert indexSettings.isSingleType();
            // This is only correct because we create types dynamically on delete operations
            // otherwise this could match the same _id from a different type
            BytesRef idBytes = Uid.encodeId(id);
            return new Term(IdFieldMapper.NAME, idBytes); // NOTE: htt, 6.0.0 之后只有 _id 只用 id字段
        } else if (indexSettings.isSingleType()) {
            // This is only correct because we create types dynamically on delete operations
            // otherwise this could match the same _id from a different type
            return new Term(IdFieldMapper.NAME, id);
        } else {
            return new Term(UidFieldMapper.NAME, Uid.createUidAsBytes(type, id));
        }
    }

    private Engine.DeleteResult delete(Engine engine, Engine.Delete delete) throws IOException { // NOTE: htt, 本地执行删除操作
        active.set(true);
        final Engine.DeleteResult result;
        delete = indexingOperationListeners.preDelete(shardId, delete); // NOTE: htt, 删除前处理
        try {
            if (logger.isTraceEnabled()) {
                logger.trace("delete [{}] (seq no [{}])", delete.uid().text(), delete.seqNo());
            }
            result = engine.delete(delete); // NOTE: htt, 引擎层执行具体删除
        } catch (Exception e) {
            indexingOperationListeners.postDelete(shardId, delete, e);
            throw e;
        }
        indexingOperationListeners.postDelete(shardId, delete, result); // NOTE: htt, 删除后处理
        return result;
    }

    public Engine.GetResult get(Engine.Get get) {
        readAllowed();
        return getEngine().get(get, this::acquireSearcher); // NOTE: htt, get处理
    }

    /**
     * Writes all indexing changes to disk and opens a new searcher reflecting all changes.  This can throw {@link AlreadyClosedException}.
     */
    public void refresh(String source) { // NOTE: htt, 将段 refresh
        verifyNotClosed();
        if (logger.isTraceEnabled()) {
            logger.trace("refresh with source [{}]", source);
        }
        getEngine().refresh(source);
    }

    /**
     * Returns how many bytes we are currently moving from heap to disk
     */
    public long getWritingBytes() {
        Engine engine = getEngineOrNull();
        if (engine == null) {
            return 0;
        }
        return engine.getWritingBytes(); // NOTE: htt, 当前从堆写入到磁盘的字节数
    }

    public RefreshStats refreshStats() {
        int listeners = refreshListeners.pendingCount();
        return new RefreshStats(refreshMetric.count(), TimeUnit.NANOSECONDS.toMillis(refreshMetric.sum()), listeners);
    }

    public FlushStats flushStats() { // NOTE: htt, flush stat统计
        return new FlushStats(flushMetric.count(), periodicFlushMetric.count(), TimeUnit.NANOSECONDS.toMillis(flushMetric.sum()));
    }

    public DocsStats docStats() {
        // we calculate the doc stats based on the internal reader that is more up-to-date and not subject
        // to external refreshes. For instance we don't refresh an external reader if we flush and indices with
        // index.refresh_interval=-1 won't see any doc stats updates at all. This change will give more accurate statistics
        // when indexing but not refreshing in general. Yet, if a refresh happens the internal reader is refresh as well so we are
        // safe here.
        long numDocs = 0;
        long numDeletedDocs = 0;
        long sizeInBytes = 0;
        try (Engine.Searcher searcher = acquireSearcher("docStats", Engine.SearcherScope.INTERNAL)) {
            for (LeafReaderContext reader : searcher.reader().leaves()) {
                // we go on the segment level here to get accurate numbers
                final SegmentReader segmentReader = Lucene.segmentReader(reader.reader()); // NOTE: htt, 段文件读取
                SegmentCommitInfo info = segmentReader.getSegmentInfo(); // NOTE: htt, 读取段信息
                numDocs += reader.reader().numDocs();
                numDeletedDocs += reader.reader().numDeletedDocs();
                try {
                    sizeInBytes += info.sizeInBytes();
                } catch (IOException e) {
                    logger.trace(() -> new ParameterizedMessage("failed to get size for [{}]", info.info.name), e);
                }
            }
        }
        return new DocsStats(numDocs, numDeletedDocs, sizeInBytes); // NOTE: htt, 文档的总数据信息
    }

    /**
     * @return {@link CommitStats} if engine is open, otherwise null
     */
    @Nullable
    public CommitStats commitStats() {
        Engine engine = getEngineOrNull();
        return engine == null ? null : engine.commitStats();
    }

    /**
     * @return {@link SeqNoStats} if engine is open, otherwise null
     */
    @Nullable
    public SeqNoStats seqNoStats() {
        Engine engine = getEngineOrNull();
        return engine == null ? null : engine.getLocalCheckpointTracker().getStats(replicationTracker.getGlobalCheckpoint()); // NOTE: htt, seqno统计信息
    }

    public IndexingStats indexingStats(String... types) {
        Engine engine = getEngineOrNull();
        final boolean throttled;
        final long throttleTimeInMillis;
        if (engine == null) {
            throttled = false;
            throttleTimeInMillis = 0;
        } else {
            throttled = engine.isThrottled();
            throttleTimeInMillis = engine.getIndexThrottleTimeInMillis();
        }
        return internalIndexingStats.stats(throttled, throttleTimeInMillis, types); // NOTE: htt, 总的index的统计信息，包括总的索引维度，以及type维度
    }

    public SearchStats searchStats(String... groups) {
        return searchStats.stats(groups); // NOTE: htt, 计算搜索的统计信息，包括第一步search以及第二步fetch的统计
    }

    public GetStats getStats() {
        return getService.stats(); // NOTE: htt, get的统计信息
    }

    public StoreStats storeStats() {
        try {
            return store.stats(); // NOTE: htt, 获取shard下所有文件的统计
        } catch (IOException e) {
            throw new ElasticsearchException("io exception while building 'store stats'", e);
        } catch (AlreadyClosedException ex) {
            return null; // already closed
        }
    }

    public MergeStats mergeStats() {
        final Engine engine = getEngineOrNull();
        if (engine == null) {
            return new MergeStats();
        }
        return engine.getMergeStats(); // NOTE: htt, merge的统计信息
    }

    public SegmentsStats segmentStats(boolean includeSegmentFileSizes) {
        SegmentsStats segmentsStats = getEngine().segmentsStats(includeSegmentFileSizes); // NOTE: htt, 段的统计信息
        segmentsStats.addBitsetMemoryInBytes(shardBitsetFilterCache.getMemorySizeInBytes());
        return segmentsStats;
    }

    public WarmerStats warmerStats() {
        return shardWarmerService.stats(); // NOTE: htt, 当前warmer的统计信息
    }

    public FieldDataStats fieldDataStats(String... fields) {
        return shardFieldData.stats(fields); // NOTE: htt, filedData的统计信息
    }

    public TranslogStats translogStats() {
        return getEngine().getTranslogStats();  // NOTE: htt, translog统计信息
    }

    public CompletionStats completionStats(String... fields) {
        CompletionStats completionStats = new CompletionStats(); // NOTE: htt, suggest的completion字段统计信息
        try (Engine.Searcher currentSearcher = acquireSearcher("completion_stats")) {
            completionStats.add(CompletionFieldStats.completionStats(currentSearcher.reader(), fields));
        }
        return completionStats;
    }

    public Engine.SyncedFlushResult syncFlush(String syncId, Engine.CommitId expectedCommitId) { // NOTE: htt, syncFlush操作
        verifyNotClosed();
        logger.trace("trying to sync flush. sync id [{}]. expected commit id [{}]]", syncId, expectedCommitId);
        Engine engine = getEngine();
        if (engine.isRecovering()) {
            throw new IllegalIndexShardStateException(shardId(), state, "syncFlush is only allowed if the engine is not recovery" +
                " from translog");
        }
        return engine.syncFlush(syncId, expectedCommitId);
    }

    /**
     * Executes the given flush request against the engine.
     *
     * @param request the flush request
     * @return the commit ID
     */
    public Engine.CommitId flush(FlushRequest request) { // NOTE: htt, 执行shard flush操作
        final boolean waitIfOngoing = request.waitIfOngoing();
        final boolean force = request.force();
        logger.trace("flush with {}", request);
        /*
         * We allow flushes while recovery since we allow operations to happen while recovering and we want to keep the translog under
         * control (up to deletes, which we do not GC). Yet, we do not use flush internally to clear deletes and flush the index writer
         * since we use Engine#writeIndexingBuffer for this now.
         */
        verifyNotClosed();
        final Engine engine = getEngine();
        if (engine.isRecovering()) {
            throw new IllegalIndexShardStateException(
                    shardId(),
                    state,
                    "flush is only allowed if the engine is not recovery from translog");
        }
        final long time = System.nanoTime();
        final Engine.CommitId commitId = engine.flush(force, waitIfOngoing); // NOTE: htt, 执行lucene的flush操作，此时会 commit()
        engine.refresh("flush"); // TODO this is technically wrong we should remove this in 7.0
        flushMetric.inc(System.nanoTime() - time);
        return commitId;
    }

    /**
     * checks and removes translog files that no longer need to be retained. See
     * {@link org.elasticsearch.index.translog.TranslogDeletionPolicy} for details
     */
    public void trimTranslog() {
        verifyNotClosed();
        final Engine engine = getEngine();
        engine.trimTranslog(); // NOTE: htt, 删除translog-xx.tlog中无用的 translog信息，这里根据translog保留时间以及大小一起判断能够保留下来的translog文件
    }

    /**
     * Rolls the tranlog generation and cleans unneeded.
     */
    private void rollTranslogGeneration() { // NOTE: htt, 滚动translog的generation
        final Engine engine = getEngine();
        engine.rollTranslogGeneration();
    }

    public void forceMerge(ForceMergeRequest forceMerge) throws IOException {
        verifyActive();
        if (logger.isTraceEnabled()) {
            logger.trace("force merge with {}", forceMerge);
        }
        Engine engine = getEngine();
        engine.forceMerge(forceMerge.flush(), forceMerge.maxNumSegments(),
            forceMerge.onlyExpungeDeletes(), false, false);
        if (forceMerge.flush()) {
            engine.refresh("force_merge"); // TODO this is technically wrong we should remove this in 7.0
        }
    }

    /**
     * Upgrades the shard to the current version of Lucene and returns the minimum segment version
     */
    public org.apache.lucene.util.Version upgrade(UpgradeRequest upgrade) throws IOException { // NOTE: htt, merge段并且提供只更新比当前lucene 版本低的段选项
        verifyActive();
        if (logger.isTraceEnabled()) {
            logger.trace("upgrade with {}", upgrade);
        }
        org.apache.lucene.util.Version previousVersion = minimumCompatibleVersion();
        // we just want to upgrade the segments, not actually forge merge to a single segment
        final Engine engine = getEngine();
        engine.forceMerge(true,  // we need to flush at the end to make sure the upgrade is durable
            Integer.MAX_VALUE, // we just want to upgrade the segments, not actually optimize to a single segment
            false, true, upgrade.upgradeOnlyAncientSegments());
        engine.refresh("upgrade"); // TODO this is technically wrong we should remove this in 7.0

        org.apache.lucene.util.Version version = minimumCompatibleVersion();
        if (logger.isTraceEnabled()) {
            logger.trace("upgraded segments for {} from version {} to version {}", shardId, previousVersion, version);
        }

        return version;
    }

    public org.apache.lucene.util.Version minimumCompatibleVersion() { // NOTE: htt, 获取段的最小的lucene 版本
        org.apache.lucene.util.Version luceneVersion = null;
        for (Segment segment : getEngine().segments(false)) {
            if (luceneVersion == null || luceneVersion.onOrAfter(segment.getVersion())) {
                luceneVersion = segment.getVersion();
            }
        }
        return luceneVersion == null ? indexSettings.getIndexVersionCreated().luceneVersion : luceneVersion;
    }

    /**
     * Creates a new {@link IndexCommit} snapshot from the currently running engine. All resources referenced by this
     * commit won't be freed until the commit / snapshot is closed.
     *
     * @param flushFirst <code>true</code> if the index should first be flushed to disk / a low level lucene commit should be executed
     */
    public Engine.IndexCommitRef acquireLastIndexCommit(boolean flushFirst) throws EngineException {
        final IndexShardState state = this.state; // one time volatile read
        // we allow snapshot on closed index shard, since we want to do one after we close the shard and before we close the engine
        if (state == IndexShardState.STARTED || state == IndexShardState.CLOSED) {
            return getEngine().acquireLastIndexCommit(flushFirst);
        } else {
            throw new IllegalIndexShardStateException(shardId, state, "snapshot is not allowed");
        }
    }

    /**
     * Snapshots the most recent safe index commit from the currently running engine.
     * All index files referenced by this index commit won't be freed until the commit/snapshot is closed.
     */
    public Engine.IndexCommitRef acquireSafeIndexCommit() throws EngineException { // NOTE: htt, 获取 index安全commit的点
        final IndexShardState state = this.state; // one time volatile read
        // we allow snapshot on closed index shard, since we want to do one after we close the shard and before we close the engine
        if (state == IndexShardState.STARTED || state == IndexShardState.CLOSED) {
            return getEngine().acquireSafeIndexCommit();
        } else {
            throw new IllegalIndexShardStateException(shardId, state, "snapshot is not allowed");
        }
    }

    /**
     * gets a {@link Store.MetadataSnapshot} for the current directory. This method is safe to call in all lifecycle of the index shard,
     * without having to worry about the current state of the engine and concurrent flushes.
     *
     * @throws org.apache.lucene.index.IndexNotFoundException     if no index is found in the current directory
     * @throws org.apache.lucene.index.CorruptIndexException      if the lucene index is corrupted. This can be caused by a checksum
     *                                                            mismatch or an unexpected exception when opening the index reading the
     *                                                            segments file.
     * @throws org.apache.lucene.index.IndexFormatTooOldException if the lucene index is too old to be opened.
     * @throws org.apache.lucene.index.IndexFormatTooNewException if the lucene index is too new to be opened.
     * @throws java.io.FileNotFoundException                      if one or more files referenced by a commit are not present.
     * @throws java.nio.file.NoSuchFileException                  if one or more files referenced by a commit are not present.
     */
    public Store.MetadataSnapshot snapshotStoreMetadata() throws IOException { // NOTE: htt, 获取每个段文件的信息
        Engine.IndexCommitRef indexCommit = null;
        store.incRef();
        try {
            Engine engine;
            synchronized (mutex) {
                // if the engine is not running, we can access the store directly, but we need to make sure no one starts
                // the engine on us. If the engine is running, we can get a snapshot via the deletion policy which is initialized.
                // That can be done out of mutex, since the engine can be closed half way.
                engine = getEngineOrNull();
                if (engine == null) {
                    return store.getMetadata(null, true);
                }
            }
            indexCommit = engine.acquireLastIndexCommit(false);
            return store.getMetadata(indexCommit.getIndexCommit()); // NOTE: htt, 获取每个段文件的信息
        } finally {
            store.decRef();
            IOUtils.close(indexCommit);
        }
    }

    /**
     * Fails the shard and marks the shard store as corrupted if
     * <code>e</code> is caused by index corruption
     */
    public void failShard(String reason, @Nullable Exception e) { // NOTE: htt, shard执行异常
        // fail the engine. This will cause this shard to also be removed from the node's index service.
        getEngine().failEngine(reason, e);
    }
    public Engine.Searcher acquireSearcher(String source) {
        return acquireSearcher(source, Engine.SearcherScope.EXTERNAL);
    }

    private Engine.Searcher acquireSearcher(String source, Engine.SearcherScope scope) { // NOTE: htt, 获取es searcher
        readAllowed();
        final Engine engine = getEngine();
        final Engine.Searcher searcher = engine.acquireSearcher(source, scope);
        boolean success = false;
        try {
            final Engine.Searcher wrappedSearcher = searcherWrapper == null ? searcher : searcherWrapper.wrap(searcher);
            assert wrappedSearcher != null;
            success = true;
            return wrappedSearcher;
        } catch (IOException ex) {
            throw new ElasticsearchException("failed to wrap searcher", ex);
        } finally {
            if (success == false) {
                Releasables.close(success, searcher);
            }
        }
    }

    public void close(String reason, boolean flushEngine) throws IOException {
        synchronized (mutex) {
            try {
                changeState(IndexShardState.CLOSED, reason); // NOTE: htt, 调整shard状态为关闭
            } finally {
                final Engine engine = this.currentEngineReference.getAndSet(null);
                try {
                    if (engine != null && flushEngine) {
                        engine.flushAndClose(); // NOTE: htt, 刷盘并关闭当前引擎，包括关闭 searchManager, translog, indexWriter, store
                    }
                } finally {
                    // playing safe here and close the engine even if the above succeeds - close can be called multiple times
                    // Also closing refreshListeners to prevent us from accumulating any more listeners
                    IOUtils.close(engine, refreshListeners);
                    indexShardOperationPermits.close();
                }
            }
        }
    }

    public IndexShard postRecovery(String reason) throws IndexShardStartedException, IndexShardRelocatedException, IndexShardClosedException {
        synchronized (mutex) {
            if (state == IndexShardState.CLOSED) {
                throw new IndexShardClosedException(shardId);
            }
            if (state == IndexShardState.STARTED) {
                throw new IndexShardStartedException(shardId);
            }
            // we need to refresh again to expose all operations that were index until now. Otherwise
            // we may not expose operations that were indexed with a refresh listener that was immediately
            // responded to in addRefreshListener.
            getEngine().refresh("post_recovery"); // NOTE: htt, recovery之后进行 refresh处理
            recoveryState.setStage(RecoveryState.Stage.DONE); // NOTE: htt, 设置恢复状态为Done
            changeState(IndexShardState.POST_RECOVERY, reason); // NOTE: htt, 改变状态为 recovery之后
        }
        return this;
    }

    /**
     * called before starting to copy index files over
     */
    public void prepareForIndexRecovery() {
        if (state != IndexShardState.RECOVERING) {
            throw new IndexShardNotRecoveringException(shardId, state);
        }
        recoveryState.setStage(RecoveryState.Stage.INDEX); // NOTE: htt, 索引恢复
        assert currentEngineReference.get() == null;
    }

    public Engine.Result applyTranslogOperation(Translog.Operation operation, Engine.Operation.Origin origin) throws IOException { // NOTE: htt, 执行 translog 进行相应操作，包括index/delete/no_op
        final Engine.Result result;
        switch (operation.opType()) {
            case INDEX:
                final Translog.Index index = (Translog.Index) operation;
                // we set canHaveDuplicates to true all the time such that we de-optimze the translog case and ensure that all
                // autoGeneratedID docs that are coming from the primary are updated correctly.
                result = applyIndexOperation(index.seqNo(), index.primaryTerm(), index.version(), // NOTE: htt, 执行translog记录的写入
                    index.versionType().versionTypeForReplicationAndRecovery(), index.getAutoGeneratedIdTimestamp(), true, origin,
                    source(shardId.getIndexName(), index.type(), index.id(), index.source(),
                        XContentHelper.xContentType(index.source())).routing(index.routing()).parent(index.parent()));
                break;
            case DELETE:
                final Translog.Delete delete = (Translog.Delete) operation;
                result = applyDeleteOperation(delete.seqNo(), delete.primaryTerm(), delete.version(), delete.type(), delete.id(),
                    delete.versionType().versionTypeForReplicationAndRecovery(), origin);
                break;
            case NO_OP:
                final Translog.NoOp noOp = (Translog.NoOp) operation;
                result = markSeqNoAsNoop(noOp.seqNo(), noOp.primaryTerm(), noOp.reason(), origin);
                break;
            default:
                throw new IllegalStateException("No operation defined for [" + operation + "]");
        }
        return result;
    }

    // package-private for testing
    int runTranslogRecovery(Engine engine, Translog.Snapshot snapshot) throws IOException { // NOTE: htt, 执行从translog的快照中进行恢复
        recoveryState.getTranslog().totalOperations(snapshot.totalOperations());
        recoveryState.getTranslog().totalOperationsOnStart(snapshot.totalOperations());
        int opsRecovered = 0;
        Translog.Operation operation;
        while ((operation = snapshot.next()) != null) {
            try {
                logger.trace("[translog] recover op {}", operation);
                Engine.Result result = applyTranslogOperation(operation, Engine.Operation.Origin.LOCAL_TRANSLOG_RECOVERY); // NOTE: htt, 应用translog
                switch (result.getResultType()) {
                    case FAILURE:
                        throw result.getFailure();
                    case MAPPING_UPDATE_REQUIRED:
                        throw new IllegalArgumentException("unexpected mapping update: " + result.getRequiredMappingUpdate());
                    case SUCCESS:
                        break;
                    default:
                        throw new AssertionError("Unknown result type [" + result.getResultType() + "]");
                }

                opsRecovered++;
                recoveryState.getTranslog().incrementRecoveredOperations();
            } catch (Exception e) {
                if (ExceptionsHelper.status(e) == RestStatus.BAD_REQUEST) {
                    // mainly for MapperParsingException and Failure to detect xcontent
                    logger.info("ignoring recovery of a corrupt translog entry", e);
                } else {
                    throw ExceptionsHelper.convertToRuntime(e);
                }
            }
        }
        return opsRecovered;
    }

    /**
     * opens the engine on top of the existing lucene engine and translog.
     * Operations from the translog will be replayed to bring lucene up to date.
     **/
    public void openEngineAndRecoverFromTranslog() throws IOException {
        innerOpenEngineAndTranslog();
        getEngine().recoverFromTranslog();
    }

    /**
     * Opens the engine on top of the existing lucene engine and translog.
     * The translog is kept but its operations won't be replayed.
     */
    public void openEngineAndSkipTranslogRecovery() throws IOException {
        innerOpenEngineAndTranslog();
        getEngine().skipTranslogRecovery();
    }

    private void innerOpenEngineAndTranslog() throws IOException { // NOTE: htt, 内部打开引擎和translog
        if (state != IndexShardState.RECOVERING) {
            throw new IndexShardNotRecoveringException(shardId, state);
        }
        recoveryState.setStage(RecoveryState.Stage.VERIFY_INDEX); // NOTE: htt, 设置状态为索引验证
        // also check here, before we apply the translog
        if (Booleans.isTrue(checkIndexOnStartup)) {
            try {
                checkIndex(); // NOTE: htt, 检查文件的checksum
            } catch (IOException ex) {
                throw new RecoveryFailedException(recoveryState, "check index failed", ex);
            }
        }
        recoveryState.setStage(RecoveryState.Stage.TRANSLOG); // NOTE: htt, translog阶段

        final EngineConfig config = newEngineConfig(); // NOTE: htt, 打开lucene引擎

        // we disable deletes since we allow for operations to be executed against the shard while recovering
        // but we need to make sure we don't loose deletes until we are done recovering
        config.setEnableGcDeletes(false);
        // we have to set it before we open an engine and recover from the translog because
        // acquiring a snapshot from the translog causes a sync which causes the global checkpoint to be pulled in,
        // and an engine can be forced to close in ctor which also causes the global checkpoint to be pulled in.
        final String translogUUID = store.readLastCommittedSegmentsInfo().getUserData().get(Translog.TRANSLOG_UUID_KEY);
        final long globalCheckpoint = Translog.readGlobalCheckpoint(translogConfig.getTranslogPath(), translogUUID);
        replicationTracker.updateGlobalCheckpointOnReplica(globalCheckpoint, "read from translog checkpoint"); // NOTE: htt, 更新checkpoint

        assertMaxUnsafeAutoIdInCommit();

        final long minRetainedTranslogGen = Translog.readMinTranslogGeneration(translogConfig.getTranslogPath(), translogUUID);
        store.trimUnsafeCommits(globalCheckpoint, minRetainedTranslogGen, config.getIndexSettings().getIndexVersionCreated());

        createNewEngine(config); // NOTE: htt, 创建新的 engine 引擎
        verifyNotClosed();
        // We set active because we are now writing operations to the engine; this way, if we go idle after some time and become inactive,
        // we still give sync'd flush a chance to run:
        active.set(true);
        assertSequenceNumbersInCommit();
        assert recoveryState.getStage() == RecoveryState.Stage.TRANSLOG : "TRANSLOG stage expected but was: " + recoveryState.getStage();
    }

    private boolean assertSequenceNumbersInCommit() throws IOException {
        final Map<String, String> userData = SegmentInfos.readLatestCommit(store.directory()).getUserData();
        assert userData.containsKey(SequenceNumbers.LOCAL_CHECKPOINT_KEY) : "commit point doesn't contains a local checkpoint";
        assert userData.containsKey(SequenceNumbers.MAX_SEQ_NO) : "commit point doesn't contains a maximum sequence number";
        assert userData.containsKey(Engine.HISTORY_UUID_KEY) : "commit point doesn't contains a history uuid";
        assert userData.get(Engine.HISTORY_UUID_KEY).equals(getHistoryUUID()) : "commit point history uuid ["
            + userData.get(Engine.HISTORY_UUID_KEY) + "] is different than engine [" + getHistoryUUID() + "]";
        return true;
    }

    private boolean assertMaxUnsafeAutoIdInCommit() throws IOException {
        final Map<String, String> userData = SegmentInfos.readLatestCommit(store.directory()).getUserData();
        if (indexSettings.getIndexVersionCreated().onOrAfter(Version.V_5_5_0)) {
            // as of 5.5.0, the engine stores the maxUnsafeAutoIdTimestamp in the commit point.
            // This should have baked into the commit by the primary we recover from, regardless of the index age.
            assert userData.containsKey(InternalEngine.MAX_UNSAFE_AUTO_ID_TIMESTAMP_COMMIT_ID) :
                "opening index which was created post 5.5.0 but " + InternalEngine.MAX_UNSAFE_AUTO_ID_TIMESTAMP_COMMIT_ID
                    + " is not found in commit";
        }
        return true;
    }

    protected void onNewEngine(Engine newEngine) {
        refreshListeners.setCurrentRefreshLocationSupplier(newEngine::getTranslogLastWriteLocation); // NOTE: htt, 记录translog最后写入位置
    }

    /**
     * called if recovery has to be restarted after network error / delay **
     */
    public void performRecoveryRestart() throws IOException {
        synchronized (mutex) {
            if (state != IndexShardState.RECOVERING) {
                throw new IndexShardNotRecoveringException(shardId, state);
            }
            assert refreshListeners.pendingCount() == 0 : "we can't restart with pending listeners";
            final Engine engine = this.currentEngineReference.getAndSet(null);
            IOUtils.close(engine);
            recoveryState().setStage(RecoveryState.Stage.INIT);
        }
    }

    /**
     * returns stats about ongoing recoveries, both source and target
     */
    public RecoveryStats recoveryStats() {
        return recoveryStats;
    }

    /**
     * Returns the current {@link RecoveryState} if this shard is recovering or has been recovering.
     * Returns null if the recovery has not yet started or shard was not recovered (created via an API).
     */
    @Override
    public RecoveryState recoveryState() {
        return this.recoveryState;
    }

    /**
     * perform the last stages of recovery once all translog operations are done.
     * note that you should still call {@link #postRecovery(String)}.
     */
    public void finalizeRecovery() { // NOTE: htt, 恢复的最后步骤
        recoveryState().setStage(RecoveryState.Stage.FINALIZE);
        Engine engine = getEngine();
        engine.refresh("recovery_finalization"); // NOTE: htt, recovery最后要将数据refresh
        engine.config().setEnableGcDeletes(true);
    }

    /**
     * Returns <tt>true</tt> if this shard can ignore a recovery attempt made to it (since the already doing/done it)
     */
    public boolean ignoreRecoveryAttempt() {
        IndexShardState state = state(); // one time volatile read
        return state == IndexShardState.POST_RECOVERY || state == IndexShardState.RECOVERING || state == IndexShardState.STARTED ||
            state == IndexShardState.CLOSED;
    }

    public void readAllowed() throws IllegalIndexShardStateException {
        IndexShardState state = this.state; // one time volatile read
        if (readAllowedStates.contains(state) == false) { // NOTE: htt, 判断当前shard是否可读
            throw new IllegalIndexShardStateException(shardId, state, "operations only allowed when shard state is one of " + readAllowedStates.toString());
        }
    }

    /** returns true if the {@link IndexShardState} allows reading */
    public boolean isReadAllowed() {
        return readAllowedStates.contains(state);
    }

    private void ensureWriteAllowed(Engine.Operation.Origin origin) throws IllegalIndexShardStateException {
        IndexShardState state = this.state; // one time volatile read

        if (origin.isRecovery()) { // NOTE: htt, recovery时的状态
            if (state != IndexShardState.RECOVERING) {
                throw new IllegalIndexShardStateException(shardId, state, "operation only allowed when recovering, origin [" + origin + "]");
            }
        } else {
            if (origin == Engine.Operation.Origin.PRIMARY) { // NOTE: htt, 主shard
                verifyPrimary();
            } else {
                assert origin == Engine.Operation.Origin.REPLICA; // NOTE: htt, 备shard
                verifyReplicationTarget();
            }
            if (writeAllowedStates.contains(state) == false) {
                throw new IllegalIndexShardStateException(shardId, state, "operation only allowed when shard state is one of " + writeAllowedStates + ", origin [" + origin + "]");
            }
        }
    }

    private void verifyPrimary() {
        if (shardRouting.primary() == false) {
            throw new IllegalStateException("shard " + shardRouting + " is not a primary");
        }
    }

    private void verifyReplicationTarget() {
        final IndexShardState state = state();
        if (shardRouting.primary() && shardRouting.active() && replicationTracker.isPrimaryMode()) {
            // must use exception that is not ignored by replication logic. See TransportActions.isShardNotAvailableException
            throw new IllegalStateException("active primary shard " + shardRouting + " cannot be a replication target before " +
                "relocation hand off, state is [" + state + "]");
        }
    }

    private void verifyNotClosed() throws IllegalIndexShardStateException {
        verifyNotClosed(null);
    }

    private void verifyNotClosed(Exception suppressed) throws IllegalIndexShardStateException {
        IndexShardState state = this.state; // one time volatile read
        if (state == IndexShardState.CLOSED) {
            final IllegalIndexShardStateException exc = new IndexShardClosedException(shardId, "operation only allowed when not closed");
            if (suppressed != null) {
                exc.addSuppressed(suppressed);
            }
            throw exc;
        }
    }

    protected final void verifyActive() throws IllegalIndexShardStateException { // NOTE: htt, shard激活则必须要started
        IndexShardState state = this.state; // one time volatile read
        if (state != IndexShardState.STARTED) {
            throw new IllegalIndexShardStateException(shardId, state, "operation only allowed when shard is active");
        }
    }

    /**
     * Returns number of heap bytes used by the indexing buffer for this shard, or 0 if the shard is closed
     */
    public long getIndexBufferRAMBytesUsed() { // NOTE: htt, 获取写入的buffer
        Engine engine = getEngineOrNull();
        if (engine == null) {
            return 0;
        }
        try {
            return engine.getIndexBufferRAMBytesUsed(); // NOTE: htt, 包括indexWriter占用内存以及 uid->version内存
        } catch (AlreadyClosedException ex) {
            return 0;
        }
    }

    public void addShardFailureCallback(Consumer<ShardFailure> onShardFailure) {
        this.shardEventListener.delegates.add(onShardFailure);
    }

    /**
     * Called by {@link IndexingMemoryController} to check whether more than {@code inactiveTimeNS} has passed since the last
     * indexing operation, and notify listeners that we are now inactive so e.g. sync'd flush can happen.
     */
    public void checkIdle(long inactiveTimeNS) { // NOTE: htt, 判断当前shard的写入时间是否超过5min，如果是则为 空闲shard
        Engine engineOrNull = getEngineOrNull();
        if (engineOrNull != null && System.nanoTime() - engineOrNull.getLastWriteNanos() >= inactiveTimeNS) {
            boolean wasActive = active.getAndSet(false);
            if (wasActive) {
                logger.debug("shard is now inactive");
                try {
                    indexEventListener.onShardInactive(this);
                } catch (Exception e) {
                    logger.warn("failed to notify index event listener", e);
                }
            }
        }
    }

    public boolean isActive() {
        return active.get();
    }

    public ShardPath shardPath() {
        return path;
    }
    // NOTE: htt, 从本地Shard的snapshot恢复shard
    public boolean recoverFromLocalShards(BiConsumer<String, MappingMetaData> mappingUpdateConsumer, List<IndexShard> localShards) throws IOException {
        assert shardRouting.primary() : "recover from local shards only makes sense if the shard is a primary shard"; // NOTE: htt, 本地恢复必须是主shard
        assert recoveryState.getRecoverySource().getType() == RecoverySource.Type.LOCAL_SHARDS : "invalid recovery type: " + recoveryState.getRecoverySource();
        final List<LocalShardSnapshot> snapshots = new ArrayList<>();
        try {
            for (IndexShard shard : localShards) {
                snapshots.add(new LocalShardSnapshot(shard)); // NOTE: htt, 获取源索引shard本地snapshot，这样直到当前快照释放，段文件才可能被删除
            }

            // we are the first primary, recover from the gateway
            // if its post api allocation, the index should exists
            assert shardRouting.primary() : "recover from local shards only makes sense if the shard is a primary shard";
            StoreRecovery storeRecovery = new StoreRecovery(shardId, logger);  // NOTE: htt, shard恢复，包括从本地磁盘或者快照中恢复
            return storeRecovery.recoverFromLocalShards(mappingUpdateConsumer, this, snapshots);  // NOTE: htt, 从本地Shard的snapshot恢复shard
        } finally {
            IOUtils.close(snapshots); // NOTE: htt, 关闭快照
        }
    }

    public boolean recoverFromStore() {
        // we are the first primary, recover from the gateway
        // if its post api allocation, the index should exists
        assert shardRouting.primary() : "recover from store only makes sense if the shard is a primary shard";
        assert shardRouting.initializing() : "can only start recovery on initializing shard";
        StoreRecovery storeRecovery = new StoreRecovery(shardId, logger);
        return storeRecovery.recoverFromStore(this); // NOTE: htt, 从本地文件恢复
    }

    public boolean restoreFromRepository(Repository repository) {
        assert shardRouting.primary() : "recover from store only makes sense if the shard is a primary shard";
        assert recoveryState.getRecoverySource().getType() == RecoverySource.Type.SNAPSHOT : "invalid recovery type: " + recoveryState.getRecoverySource();
        StoreRecovery storeRecovery = new StoreRecovery(shardId, logger);
        return storeRecovery.recoverFromRepository(this, repository); // NOTE; htt, 从快照文件中恢复
    }

    /**
     * Tests whether or not the engine should be flushed periodically.
     * This test is based on the current size of the translog compared to the configured flush threshold size.
     *
     * @return {@code true} if the engine should be flushed
     */
    boolean shouldPeriodicallyFlush() {
        final Engine engine = getEngineOrNull();
        if (engine != null) {
            try {
                return engine.shouldPeriodicallyFlush(); // NOTE: htt, 是否定期flush
            } catch (final AlreadyClosedException e) {
                // we are already closed, no need to flush or roll
            }
        }
        return false;
    }

    /**
     * Tests whether or not the translog generation should be rolled to a new generation. This test is based on the size of the current
     * generation compared to the configured generation threshold size.
     *
     * @return {@code true} if the current generation should be rolled to a new generation
     */
    boolean shouldRollTranslogGeneration() {
        final Engine engine = getEngineOrNull();
        if (engine != null) {
            try {
                return engine.shouldRollTranslogGeneration(); // NOTE: htt，是否增加translog的generation
            } catch (final AlreadyClosedException e) {
                // we are already closed, no need to flush or roll
            }
        }
        return false;
    }

    public void onSettingsChanged() { // NOTE: htt, 更新lucene的merge的个数和线程数信息，更新translog的配置信息（最大保留时间和保留文件大小）
        Engine engineOrNull = getEngineOrNull();
        if (engineOrNull != null) {
            engineOrNull.onSettingsChanged();
        }
    }

    /**
     * Acquires a lock on the translog files, preventing them from being trimmed.
     */
    public Closeable acquireTranslogRetentionLock() {
        return getEngine().acquireTranslogRetentionLock(); // NOTE: emep, translog file的锁
    }

    /**
     * Creates a new translog snapshot for reading translog operations whose seq# at least the provided seq#.
     * The caller has to close the returned snapshot after finishing the reading.
     */
    public Translog.Snapshot newTranslogSnapshotFromMinSeqNo(long minSeqNo) throws IOException {
        return getEngine().newTranslogSnapshotFromMinSeqNo(minSeqNo); // NOTE: htt, 获取新的translog的快照
    }

    /**
     * Returns the estimated number of operations in translog whose seq# at least the provided seq#.
     */
    public int estimateTranslogOperationsFromMinSeq(long minSeqNo) {
        return getEngine().estimateTranslogOperationsFromMinSeq(minSeqNo); // NOTE: htt, 评估从minSeqNo开始总共的操作数
    }

    public List<Segment> segments(boolean verbose) {
        return getEngine().segments(verbose);
    }

    public void flushAndCloseEngine() throws IOException {
        getEngine().flushAndClose();
    }

    public String getHistoryUUID() {
        return getEngine().getHistoryUUID();
    }

    public IndexEventListener getIndexEventListener() {
        return indexEventListener;
    }

    public void activateThrottling() {
        try {
            getEngine().activateThrottling(); // NOTE: htt, 激活限流
        } catch (AlreadyClosedException ex) {
            // ignore
        }
    }

    public void deactivateThrottling() {
        try {
            getEngine().deactivateThrottling(); // NOTE: htt, 解除限流
        } catch (AlreadyClosedException ex) {
            // ignore
        }
    }

    private void handleRefreshException(Exception e) {
        if (e instanceof AlreadyClosedException) {
            // ignore
        } else if (e instanceof RefreshFailedEngineException) {
            RefreshFailedEngineException rfee = (RefreshFailedEngineException) e;
            if (rfee.getCause() instanceof InterruptedException) {
                // ignore, we are being shutdown
            } else if (rfee.getCause() instanceof ClosedByInterruptException) {
                // ignore, we are being shutdown
            } else if (rfee.getCause() instanceof ThreadInterruptedException) {
                // ignore, we are being shutdown
            } else {
                if (state != IndexShardState.CLOSED) {
                    logger.warn("Failed to perform engine refresh", e);
                }
            }
        } else {
            if (state != IndexShardState.CLOSED) {
                logger.warn("Failed to perform engine refresh", e);
            }
        }
    }

    /**
     * Called when our shard is using too much heap and should move buffered indexed/deleted documents to disk.
     */
    public void writeIndexingBuffer() {
        try {
            Engine engine = getEngine();
            engine.writeIndexingBuffer(); // NOTE: htt, 处理当前 index 使用buffer过多，当前处理是refresh
        } catch (Exception e) {
            handleRefreshException(e);
        }
    }

    /**
     * Notifies the service to update the local checkpoint for the shard with the provided allocation ID. See
     * {@link ReplicationTracker#updateLocalCheckpoint(String, long)} for
     * details.
     *
     * @param allocationId the allocation ID of the shard to update the local checkpoint for
     * @param checkpoint   the local checkpoint for the shard
     */
    public void updateLocalCheckpointForShard(final String allocationId, final long checkpoint) {
        verifyPrimary();
        verifyNotClosed();
        replicationTracker.updateLocalCheckpoint(allocationId, checkpoint); // NOTE: htt, 更新本地checkpoint
    }

    /**
     * Update the local knowledge of the global checkpoint for the specified allocation ID.
     *
     * @param allocationId     the allocation ID to update the global checkpoint for
     * @param globalCheckpoint the global checkpoint
     */
    public void updateGlobalCheckpointForShard(final String allocationId, final long globalCheckpoint) {
        verifyPrimary();
        verifyNotClosed();
        replicationTracker.updateGlobalCheckpointForShard(allocationId, globalCheckpoint); // NOTE: htt, 更新主shard的 globalCheckpoint
    }

    /**
     * Waits for all operations up to the provided sequence number to complete.
     *
     * @param seqNo the sequence number that the checkpoint must advance to before this method returns
     * @throws InterruptedException if the thread was interrupted while blocking on the condition
     */
    public void waitForOpsToComplete(final long seqNo) throws InterruptedException {
        getEngine().getLocalCheckpointTracker().waitForOpsToComplete(seqNo); // NOTE: htt, 等待本地checkpoint同步到 seqNo
    }

    /**
     * Called when the recovery process for a shard has opened the engine on the target shard. Ensures that the right data structures
     * have been set up locally to track local checkpoint information for the shard and that the shard is added to the replication group.
     *
     * @param allocationId  the allocation ID of the shard for which recovery was initiated
     */
    public void initiateTracking(final String allocationId) {
        verifyPrimary();
        replicationTracker.initiateTracking(allocationId); // NOTE: htt, 将shard的allocationId节点加入track，即会同步数据
    }

    /**
     * Marks the shard with the provided allocation ID as in-sync with the primary shard. See
     * {@link ReplicationTracker#markAllocationIdAsInSync(String, long)}
     * for additional details.
     *
     * @param allocationId    the allocation ID of the shard to mark as in-sync
     * @param localCheckpoint the current local checkpoint on the shard
     */
    public void markAllocationIdAsInSync(final String allocationId, final long localCheckpoint) throws InterruptedException {
        verifyPrimary();
        replicationTracker.markAllocationIdAsInSync(allocationId, localCheckpoint); // NOTE: htt, 标记当前的localCheckPoint以及在in-sync中，如果localCheckpoint数据比globalCheckpoint小则会一直等待
    }

    /**
     * Returns the local checkpoint for the shard.
     *
     * @return the local checkpoint
     */
    public long getLocalCheckpoint() {
        return getEngine().getLocalCheckpointTracker().getCheckpoint(); // NOTE: htt, 当前shard的本地的checkPoint
    }

    /**
     * Returns the global checkpoint for the shard.
     *
     * @return the global checkpoint
     */
    public long getGlobalCheckpoint() {
        return replicationTracker.getGlobalCheckpoint(); // NOTE: htt, 当前shard的globalCheckpoint
    }

    /**
     * Returns the latest global checkpoint value that has been persisted in the underlying storage (i.e. translog's checkpoint)
     */
    public long getLastSyncedGlobalCheckpoint() {
        return getEngine().getLastSyncedGlobalCheckpoint(); // NOTE: htt, 最近一次同步的globalCheckpoint
    }

    /**
     * Get the local knowledge of the global checkpoints for all in-sync allocation IDs.
     *
     * @return a map from allocation ID to the local knowledge of the global checkpoint for that allocation ID
     */
    public ObjectLongMap<String> getInSyncGlobalCheckpoints() {
        verifyPrimary();
        verifyNotClosed();
        return replicationTracker.getInSyncGlobalCheckpoints(); // NOTE: htt, 获得 in-sync副本的globalCheckpoint
    }

    /**
     * Syncs the global checkpoint to the replicas if the global checkpoint on at least one replica is behind the global checkpoint on the
     * primary.
     */
    public void maybeSyncGlobalCheckpoint(final String reason) { // NOTE: htt, 如果有副本globaCheckpoint比当前主shard小，则发起同步
        verifyPrimary();
        verifyNotClosed();
        if (replicationTracker.isPrimaryMode() == false) {
            return;
        }
        // only sync if there are not operations in flight
        final SeqNoStats stats = getEngine().getLocalCheckpointTracker().getStats(replicationTracker.getGlobalCheckpoint());
        if (stats.getMaxSeqNo() == stats.getGlobalCheckpoint()) {
            final ObjectLongMap<String> globalCheckpoints = getInSyncGlobalCheckpoints();
            final String allocationId = routingEntry().allocationId().getId();
            assert globalCheckpoints.containsKey(allocationId);
            final long globalCheckpoint = globalCheckpoints.get(allocationId); // NOTE: htt, 当前主shard副本的globalCheckpoint
            final boolean syncNeeded =
                    StreamSupport
                            .stream(globalCheckpoints.values().spliterator(), false)
                            .anyMatch(v -> v.value < globalCheckpoint); // NOTE: htt, 如果有副本，器globalCheckpint比当前主shard对应的globalCheckpoint小, 则需要同步
            // only sync if there is a shard lagging the primary
            if (syncNeeded) {
                logger.trace("syncing global checkpoint for [{}]", reason);
                globalCheckpointSyncer.run(); // NOTE: htt, globalCheckpoint同步，副本同步备操作 GlobalCheckpointSyncAction.updateGlobalCheckpointForShard
            }
        }
    }

    /**
     * Returns the current replication group for the shard.
     *
     * @return the replication group
     */
    public ReplicationGroup getReplicationGroup() {
        verifyPrimary();
        verifyNotClosed();
        return replicationTracker.getReplicationGroup();  // NOTE: htt, 复制组，包括未分配的shardRouting，迁移目标的shardRouting
    }

    /**
     * Updates the global checkpoint on a replica shard after it has been updated by the primary.
     *
     * @param globalCheckpoint the global checkpoint
     * @param reason           the reason the global checkpoint was updated
     */
    public void updateGlobalCheckpointOnReplica(final long globalCheckpoint, final String reason) { // NOTE: htt, 副本上更新 globalCheckpoint
        verifyReplicationTarget();
        final long localCheckpoint = getEngine().getLocalCheckpointTracker().getCheckpoint();
        if (globalCheckpoint > localCheckpoint) { // NOTE: htt, globalCheckpoint不应该比localCheckPoint，除非当前正在恢复中
            /*
             * This can happen during recovery when the shard has started its engine but recovery is not finalized and is receiving global
             * checkpoint updates. However, since this shard is not yet contributing to calculating the global checkpoint, it can be the
             * case that the global checkpoint update from the primary is ahead of the local checkpoint on this shard. In this case, we
             * ignore the global checkpoint update. This can happen if we are in the translog stage of recovery. Prior to this, the engine
             * is not opened and this shard will not receive global checkpoint updates, and after this the shard will be contributing to
             * calculations of the global checkpoint. However, we can not assert that we are in the translog stage of recovery here as
             * while the global checkpoint update may have emanated from the primary when we were in that state, we could subsequently move
             * to recovery finalization, or even finished recovery before the update arrives here.
             */
            assert state() != IndexShardState.POST_RECOVERY && state() != IndexShardState.STARTED :
                "supposedly in-sync shard copy received a global checkpoint [" + globalCheckpoint + "] " +
                    "that is higher than its local checkpoint [" + localCheckpoint + "]";
            return;
        }
        replicationTracker.updateGlobalCheckpointOnReplica(globalCheckpoint, reason); // NOTE: htt, 更新本地的globalCheckpoint
    }

    /**
     * Updates the known allocation IDs and the local checkpoints for the corresponding allocations from a primary relocation source.
     *
     * @param primaryContext the sequence number context
     */
    public void activateWithPrimaryContext(final ReplicationTracker.PrimaryContext primaryContext) { // NOTE: htt, 激活当前shard为主shard，并采用旧主shard的context来更新
        verifyPrimary();
        assert shardRouting.isRelocationTarget() : "only relocation target can update allocation IDs from primary context: " + shardRouting;
        assert primaryContext.getCheckpointStates().containsKey(routingEntry().allocationId().getId()) &&
            getEngine().getLocalCheckpointTracker().getCheckpoint() ==
                primaryContext.getCheckpointStates().get(routingEntry().allocationId().getId()).getLocalCheckpoint();
        synchronized (mutex) {
            replicationTracker.activateWithPrimaryContext(primaryContext); // make changes to primaryMode flag only under mutex // NOTE: htt, 根据旧主shard提供的 primaryContext来更新当前的新主shard内容
        }
    }

    /**
     * Check if there are any recoveries pending in-sync.
     *
     * @return {@code true} if there is at least one shard pending in-sync, otherwise false
     */
    public boolean pendingInSync() {
        verifyPrimary();
        return replicationTracker.pendingInSync(); // NOTE: htt, 检查是否有等待in-sync中的恢复
    }

    /**
     * Should be called for each no-op update operation to increment relevant statistics.
     *
     * @param type the doc type of the update
     */
    public void noopUpdate(String type) {
        internalIndexingStats.noopUpdate(type);
    }

    void checkIndex() throws IOException {
        if (store.tryIncRef()) {
            try {
                doCheckIndex();
            } finally {
                store.decRef();
            }
        }
    }

    private void doCheckIndex() throws IOException {
        long timeNS = System.nanoTime();
        if (!Lucene.indexExists(store.directory())) {
            return;
        }
        BytesStreamOutput os = new BytesStreamOutput();
        PrintStream out = new PrintStream(os, false, StandardCharsets.UTF_8.name());

        if ("checksum".equals(checkIndexOnStartup)) { // NOTE: htt, 检查checksum
            // physical verification only: verify all checksums for the latest commit
            IOException corrupt = null;
            MetadataSnapshot metadata = snapshotStoreMetadata();
            for (Map.Entry<String, StoreFileMetaData> entry : metadata.asMap().entrySet()) {
                try {
                    Store.checkIntegrity(entry.getValue(), store.directory()); // NOTE: htt, 检测每个文件的完整性
                    out.println("checksum passed: " + entry.getKey());
                } catch (IOException exc) {
                    out.println("checksum failed: " + entry.getKey());
                    exc.printStackTrace(out);
                    corrupt = exc;
                }
            }
            out.flush();
            if (corrupt != null) {
                logger.warn("check index [failure]\n{}", os.bytes().utf8ToString());
                throw corrupt;
            }
        } else {
            // full checkindex
            final CheckIndex.Status status = store.checkIndex(out);
            out.flush();
            if (!status.clean) {
                if (state == IndexShardState.CLOSED) {
                    // ignore if closed....
                    return;
                }
                logger.warn("check index [failure]\n{}", os.bytes().utf8ToString());
                if ("fix".equals(checkIndexOnStartup)) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("fixing index, writing new segments file ...");
                    }
                    store.exorciseIndex(status);
                    if (logger.isDebugEnabled()) {
                        logger.debug("index fixed, wrote new segments file \"{}\"", status.segmentsFileName);
                    }
                } else {
                    // only throw a failure if we are not going to fix the index
                    throw new IllegalStateException("index check failure but can't fix it");
                }
            }
        }

        if (logger.isDebugEnabled()) {
            logger.debug("check index [success]\n{}", os.bytes().utf8ToString());
        }

        recoveryState.getVerifyIndex().checkIndexTime(Math.max(0, TimeValue.nsecToMSec(System.nanoTime() - timeNS)));
    }

    Engine getEngine() {
        Engine engine = getEngineOrNull(); // NOTE: htt, 返回当前的引擎
        if (engine == null) {
            throw new AlreadyClosedException("engine is closed");
        }
        return engine;
    }

    /**
     * NOTE: returns null if engine is not yet started (e.g. recovery phase 1, copying over index files, is still running), or if engine is
     * closed.
     */
    protected Engine getEngineOrNull() {
        return this.currentEngineReference.get();
    }

    public void startRecovery(RecoveryState recoveryState, PeerRecoveryTargetService recoveryTargetService,
                              PeerRecoveryTargetService.RecoveryListener recoveryListener, RepositoriesService repositoriesService,
                              BiConsumer<String, MappingMetaData> mappingUpdateConsumer,
                              IndicesService indicesService) {
        // TODO: Create a proper object to encapsulate the recovery context
        // all of the current methods here follow a pattern of:
        // resolve context which isn't really dependent on the local shards and then async
        // call some external method with this pointer.
        // with a proper recovery context object we can simply change this to:
        // startRecovery(RecoveryState recoveryState, ShardRecoverySource source ) {
        //     markAsRecovery("from " + source.getShortDescription(), recoveryState);
        //     threadPool.generic().execute()  {
        //           onFailure () { listener.failure() };
        //           doRun() {
        //                if (source.recover(this)) {
        //                  recoveryListener.onRecoveryDone(recoveryState);
        //                }
        //           }
        //     }}
        // }
        assert recoveryState.getRecoverySource().equals(shardRouting.recoverySource());
        switch (recoveryState.getRecoverySource().getType()) {
            case EMPTY_STORE:
            case EXISTING_STORE:
                markAsRecovering("from store", recoveryState); // mark the shard as recovering on the cluster state thread
                threadPool.generic().execute(() -> {
                    try {
                        if (recoverFromStore()) { // NOTE: htt, 本地恢复，包括段文件+translog
                            recoveryListener.onRecoveryDone(recoveryState);
                        }
                    } catch (Exception e) {
                        recoveryListener.onRecoveryFailure(recoveryState, new RecoveryFailedException(recoveryState, null, e), true);
                    }
                });
                break;
            case PEER:
                try {
                    markAsRecovering("from " + recoveryState.getSourceNode(), recoveryState);
                    recoveryTargetService.startRecovery(this, recoveryState.getSourceNode(), recoveryListener); // NOTE: htt, 执行START_RECOVERY，开始从远程主shard恢复
                } catch (Exception e) {
                    failShard("corrupted preexisting index", e);
                    recoveryListener.onRecoveryFailure(recoveryState, new RecoveryFailedException(recoveryState, null, e), true);
                }
                break;
            case SNAPSHOT:
                markAsRecovering("from snapshot", recoveryState); // mark the shard as recovering on the cluster state thread
                SnapshotRecoverySource recoverySource = (SnapshotRecoverySource) recoveryState.getRecoverySource();
                threadPool.generic().execute(() -> {
                    try {
                        final Repository repository = repositoriesService.repository(recoverySource.snapshot().getRepository()); // NOTE: htt, 获取快照仓库
                        if (restoreFromRepository(repository)) { // NOTE: htt, 从快照恢复数据（只能主shard从快速恢复）
                            recoveryListener.onRecoveryDone(recoveryState);
                        }
                    } catch (Exception e) {
                        recoveryListener.onRecoveryFailure(recoveryState, new RecoveryFailedException(recoveryState, null, e), true);
                    }
                });
                break;
            case LOCAL_SHARDS:
                final IndexMetaData indexMetaData = indexSettings().getIndexMetaData();
                final Index resizeSourceIndex = indexMetaData.getResizeSourceIndex();
                final List<IndexShard> startedShards = new ArrayList<>(); // NOTE: htt, 需要的源索引shardid中已经启动的shard列表
                final IndexService sourceIndexService = indicesService.indexService(resizeSourceIndex);
                final Set<ShardId> requiredShards; // NOTE: htt, 获取索引或吃饭时目标shardid对应源索引的shardId列表
                final int numShards;
                if (sourceIndexService != null) {
                    requiredShards = IndexMetaData.selectRecoverFromShards(shardId().id(),
                        sourceIndexService.getMetaData(), indexMetaData.getNumberOfShards()); // NOTE: htt, 获取索引或吃饭时目标shardid对应源索引的shardid列表
                    for (IndexShard shard : sourceIndexService) {
                        if (shard.state() == IndexShardState.STARTED && requiredShards.contains(shard.shardId())) {
                            startedShards.add(shard); // NOTE: htt, 源索引shardid中已经启动的shard列表
                        }
                    }
                    numShards = requiredShards.size(); // NOTE: htt, 目标shardid需要的源shardid列表
                } else {
                    numShards = -1;
                    requiredShards = Collections.emptySet();
                }

                if (numShards == startedShards.size()) {
                    assert requiredShards.isEmpty() == false;
                    markAsRecovering("from local shards", recoveryState); // mark the shard as recovering on the cluster state thread
                    threadPool.generic().execute(() -> {
                        try {
                            if (recoverFromLocalShards(mappingUpdateConsumer, startedShards.stream()
                                .filter((s) -> requiredShards.contains(s.shardId())).collect(Collectors.toList()))) {  // NOTE: htt, 从本地Shard的snapshot恢复shard
                                recoveryListener.onRecoveryDone(recoveryState); // NOTE: htt, 恢复成功
                            }
                        } catch (Exception e) {
                            recoveryListener.onRecoveryFailure(recoveryState,
                                new RecoveryFailedException(recoveryState, null, e), true);
                        }
                    });
                } else { // NOTE: htt, 目标索引shardid需要的源索引shardid都必须启动
                    final RuntimeException e;
                    if (numShards == -1) {
                        e = new IndexNotFoundException(resizeSourceIndex);
                    } else {
                        e = new IllegalStateException("not all required shards of index " + resizeSourceIndex
                            + " are started yet, expected " + numShards + " found " + startedShards.size() + " can't recover shard "
                            + shardId());
                    }
                    throw e;
                }
                break;
            default:
                throw new IllegalArgumentException("Unknown recovery source " + recoveryState.getRecoverySource());
        }
    }

    /**
     * Returns whether the shard is in primary mode, i.e., in charge of replicating changes (see {@link ReplicationTracker}).
     */
    public boolean isPrimaryMode() {
        return replicationTracker.isPrimaryMode();
    }

    class ShardEventListener implements Engine.EventListener { // NOTE: htt, shard 异常事件监听
        private final CopyOnWriteArrayList<Consumer<ShardFailure>> delegates = new CopyOnWriteArrayList<>(); // NOTE: htt, 代理事件

        // called by the current engine
        @Override
        public void onFailedEngine(String reason, @Nullable Exception failure) {
            final ShardFailure shardFailure = new ShardFailure(shardRouting, reason, failure);
            for (Consumer<ShardFailure> listener : delegates) {
                try {
                    listener.accept(shardFailure); // NOTE: htt, 遍历代理的异常类并执行
                } catch (Exception inner) {
                    inner.addSuppressed(failure);
                    logger.warn("exception while notifying engine failure", inner);
                }
            }
        }
    }

    private Engine createNewEngine(EngineConfig config) { // NOTE: htt, 创建新的引起
        synchronized (mutex) {
            if (state == IndexShardState.CLOSED) {
                throw new AlreadyClosedException(shardId + " can't create engine - shard is closed");
            }
            assert this.currentEngineReference.get() == null;
            Engine engine = newEngine(config); // NOTE: htt, 创建新的引擎
            onNewEngine(engine); // call this before we pass the memory barrier otherwise actions that happen
            // inside the callback are not visible. This one enforces happens-before
            this.currentEngineReference.set(engine); // NOTE: htt, 设置为当前的engine
        }

        // time elapses after the engine is created above (pulling the config settings) until we set the engine reference, during which
        // settings changes could possibly have happened, so here we forcefully push any config changes to the new engine:
        Engine engine = getEngineOrNull();

        // engine could perhaps be null if we were e.g. concurrently closed:
        if (engine != null) {
            engine.onSettingsChanged(); // NOTE: htt, 触发引擎的setting变更操作，这里包含translog的配置变更
        }
        return engine;
    }

    protected Engine newEngine(EngineConfig config) {
        return engineFactory.newReadWriteEngine(config); // NOTE: htt, 创建新的engine，包括打开shard，创建lucene的indexWriter等
    }

    private static void persistMetadata( // NOTE: htt, 持久化index shard的元信息，会判断底层是否已经持久化，如果已持久化并未发生变化则才会持久化，降低底层IO
            final ShardPath shardPath,
            final IndexSettings indexSettings,
            final ShardRouting newRouting,
            final @Nullable ShardRouting currentRouting,
            final Logger logger) throws IOException {
        assert newRouting != null : "newRouting must not be null";

        // only persist metadata if routing information that is persisted in shard state metadata actually changed
        final ShardId shardId = newRouting.shardId();
        if (currentRouting == null
            || currentRouting.primary() != newRouting.primary()
            || currentRouting.allocationId().equals(newRouting.allocationId()) == false) { // NOTE:htt, 之前routing为空，或者新老routing primary不一致，或者allocationId不一致才更新，否则不更新
            assert currentRouting == null || currentRouting.isSameAllocation(newRouting);
            final String writeReason;
            if (currentRouting == null) {
                writeReason = "initial state with allocation id [" + newRouting.allocationId() + "]";
            } else {
                writeReason = "routing changed from " + currentRouting + " to " + newRouting;
            }
            logger.trace("{} writing shard state, reason [{}]", shardId, writeReason);
            final ShardStateMetaData newShardStateMetadata =
                    new ShardStateMetaData(newRouting.primary(), indexSettings.getUUID(), newRouting.allocationId());
            ShardStateMetaData.FORMAT.write(newShardStateMetadata, shardPath.getShardStatePath()); // NOTE: htt, 记录shard的元信息到${shard_path}/_state/state-xx.st文件中
        } else { // NOTE:htt, 如果新老routing一致，则不更新，即集群状态更新的时候并不是每次都更新，只有涉及到shard状态变更时才更新本地文件
            logger.trace("{} skip writing shard state, has been written before", shardId);
        }
    }

    private DocumentMapperForType docMapper(String type) {
        return mapperService.documentMapperWithAutoCreate(type); // NOTE: htt, 如果mapping不存在，则解析mapping，以便自动创建
    }

    private EngineConfig newEngineConfig() {
        Sort indexSort = indexSortSupplier.get();
        return new EngineConfig(shardId, shardRouting.allocationId().getId(),
            threadPool, indexSettings, warmer, store, indexSettings.getMergePolicy(), // NOTE: htt, 引擎配置中包括 store
            mapperService.indexAnalyzer(), similarityService.similarity(mapperService), codecService, shardEventListener,
            indexCache.query(), cachingPolicy, translogConfig,
            IndexingMemoryController.SHARD_INACTIVE_TIME_SETTING.get(indexSettings.getSettings()), // NOTE: htt, 5min不活跃则标记shard为idle
            Collections.singletonList(refreshListeners),
            Collections.singletonList(new RefreshMetricUpdater(refreshMetric)),
            indexSort, this::runTranslogRecovery, circuitBreakerService, replicationTracker, this::getPrimaryTerm);
    }

    /**
     * Acquire a primary operation permit whenever the shard is ready for indexing. If a permit is directly available, the provided
     * ActionListener will be called on the calling thread. During relocation hand-off, permit acquisition can be delayed. The provided
     * ActionListener will then be called using the provided executor.
     *
     * @param debugInfo an extra information that can be useful when tracing an unreleased permit. When assertions are enabled
     *                  the tracing will capture the supplied object's {@link Object#toString()} value. Otherwise the object
     *                  isn't used
     */
    public void acquirePrimaryOperationPermit(ActionListener<Releasable> onPermitAcquired, String executorOnDelay, Object debugInfo) {
        verifyNotClosed();
        verifyPrimary();

        indexShardOperationPermits.acquire(onPermitAcquired, executorOnDelay, false, debugInfo); // NOTE: htt, 如果当前主shard ready则执行操作，如果在hand-off则会延迟
    }

    private final Object primaryTermMutex = new Object(); // NOTE: htt, 主shard下的锁

    /**
     * Acquire a replica operation permit whenever the shard is ready for indexing (see
     * {@link #acquirePrimaryOperationPermit(ActionListener, String, Object)}). If the given primary term is lower than then one in
     * {@link #shardRouting}, the {@link ActionListener#onFailure(Exception)} method of the provided listener is invoked with an
     * {@link IllegalStateException}. If permit acquisition is delayed, the listener will be invoked on the executor with the specified
     * name.
     *
     * @param operationPrimaryTerm the operation primary term
     * @param globalCheckpoint     the global checkpoint associated with the request
     * @param onPermitAcquired     the listener for permit acquisition
     * @param executorOnDelay      the name of the executor to invoke the listener on if permit acquisition is delayed
     * @param debugInfo            an extra information that can be useful when tracing an unreleased permit. When assertions are enabled
     *                             the tracing will capture the supplied object's {@link Object#toString()} value. Otherwise the object
     *                             isn't used
     */
    public void acquireReplicaOperationPermit(final long operationPrimaryTerm, final long globalCheckpoint,
                                              final ActionListener<Releasable> onPermitAcquired, final String executorOnDelay,
                                              final Object debugInfo) { // NOTE: htt, 更新当前shard的glbalCheckpoint以及localCheckpoint
        verifyNotClosed();
        verifyReplicationTarget();
        final boolean globalCheckpointUpdated;
        if (operationPrimaryTerm > primaryTerm) { // NOTE: htt, 比当前的primaryTerm大
            synchronized (primaryTermMutex) {
                if (operationPrimaryTerm > primaryTerm) {
                    IndexShardState shardState = state();
                    // only roll translog and update primary term if shard has made it past recovery
                    // Having a new primary term here means that the old primary failed and that there is a new primary, which again
                    // means that the master will fail this shard as all initializing shards are failed when a primary is selected
                    // We abort early here to prevent an ongoing recovery from the failed primary to mess with the global / local checkpoint
                    if (shardState != IndexShardState.POST_RECOVERY &&
                        shardState != IndexShardState.STARTED) {
                        throw new IndexShardNotStartedException(shardId, shardState);
                    }
                    try {
                        indexShardOperationPermits.blockOperations(30, TimeUnit.MINUTES, () -> { // NOTE: htt, 最多阻塞30min执行
                            assert operationPrimaryTerm > primaryTerm :
                                "shard term already update.  op term [" + operationPrimaryTerm + "], shardTerm [" + primaryTerm + "]";
                            primaryTerm = operationPrimaryTerm; // NOTE: htt, 更新当前shard主term
                            updateGlobalCheckpointOnReplica(globalCheckpoint, "primary term transition"); // NOTE: htt, 更新主shard同步的globalCheckpoint
                            final long currentGlobalCheckpoint = getGlobalCheckpoint();
                            final long localCheckpoint;
                            if (currentGlobalCheckpoint == SequenceNumbers.UNASSIGNED_SEQ_NO) {
                                localCheckpoint = SequenceNumbers.NO_OPS_PERFORMED;
                            } else {
                                localCheckpoint = currentGlobalCheckpoint; // NOTE: htt, 将localCheckpoint调整为globalCheckpoint，此时可能会降低localCheckpoint
                            }
                            logger.trace(
                                    "detected new primary with primary term [{}], resetting local checkpoint from [{}] to [{}]",
                                    operationPrimaryTerm,
                                    getLocalCheckpoint(),
                                    localCheckpoint);
                            getEngine().getLocalCheckpointTracker().resetCheckpoint(localCheckpoint); // NOTE: htt, 更新本地的记录的localCheckpoint
                            getEngine().rollTranslogGeneration(); // NOTE: htt, 滚动translog，旧的translog会刷盘处理
                        });
                        globalCheckpointUpdated = true;
                    } catch (final Exception e) {
                        onPermitAcquired.onFailure(e); // NOTE: htt, 没有获取到执行权限
                        return;
                    }
                } else {
                    globalCheckpointUpdated = false;
                }
            }
        } else {
            globalCheckpointUpdated = false;
        }

        assert operationPrimaryTerm <= primaryTerm
                : "operation primary term [" + operationPrimaryTerm + "] should be at most [" + primaryTerm + "]";
        indexShardOperationPermits.acquire(  // NOTE: htt, 执行回复处理
                new ActionListener<Releasable>() {
                    @Override
                    public void onResponse(final Releasable releasable) {
                        if (operationPrimaryTerm < primaryTerm) {
                            releasable.close();
                            final String message = String.format(
                                    Locale.ROOT,
                                    "%s operation primary term [%d] is too old (current [%d])",
                                    shardId,
                                    operationPrimaryTerm,
                                    primaryTerm);
                            onPermitAcquired.onFailure(new IllegalStateException(message));
                        } else {
                            if (globalCheckpointUpdated == false) {
                                try {
                                    updateGlobalCheckpointOnReplica(globalCheckpoint, "operation"); // NOTE: htt, 更新本地的globalCheckpoint
                                } catch (Exception e) {
                                    releasable.close();
                                    onPermitAcquired.onFailure(e);
                                    return;
                                }
                            }
                            onPermitAcquired.onResponse(releasable);
                        }
                    }

                    @Override
                    public void onFailure(final Exception e) {
                        onPermitAcquired.onFailure(e);
                    }
                },
                executorOnDelay,
                true, debugInfo);
    }

    public int getActiveOperationsCount() { // NOTE: htt, 获得激活操作的个数
        return indexShardOperationPermits.getActiveOperationsCount(); // refCount is incremented on successful acquire and decremented on close
    }

    /**
     * @return a list of describing each permit that wasn't released yet. The description consist of the debugInfo supplied
     *         when the permit was acquired plus a stack traces that was captured when the permit was request.
     */
    public List<String> getActiveOperations() {
        return indexShardOperationPermits.getActiveOperations();
    }

    private final AsyncIOProcessor<Translog.Location> translogSyncProcessor = new AsyncIOProcessor<Translog.Location>(logger, 1024) { // NOTE: htt, translog构建批量异步刷盘
        @Override
        protected void write(List<Tuple<Translog.Location, Consumer<Exception>>> candidates) throws IOException {
            try {
                getEngine().ensureTranslogSynced(candidates.stream().map(Tuple::v1)); // NOTE: htt, translog刷盘，找到最大的位置进行刷新
            } catch (AlreadyClosedException ex) {
                // that's fine since we already synced everything on engine close - this also is conform with the methods
                // documentation
            } catch (IOException ex) { // if this fails we are in deep shit - fail the request
                logger.debug("failed to sync translog", ex);
                throw ex;
            }
        }
    };

    /**
     * Syncs the given location with the underlying storage unless already synced. This method might return immediately without
     * actually fsyncing the location until the sync listener is called. Yet, unless there is already another thread fsyncing
     * the transaction log the caller thread will be hijacked to run the fsync for all pending fsync operations.
     * This method allows indexing threads to continue indexing without blocking on fsync calls. We ensure that there is only
     * one thread blocking on the sync an all others can continue indexing.
     * NOTE: if the syncListener throws an exception when it's processed the exception will only be logged. Users should make sure that the
     * listener handles all exception cases internally.
     */
    public final void sync(Translog.Location location, Consumer<Exception> syncListener) { // NOTE: htt, 批量刷盘translog
        verifyNotClosed();
        translogSyncProcessor.put(location, syncListener); // NOTE: htt, translog批量刷盘
    }

    public void sync() throws IOException { // NOTE: htt, 执行translog的sync刷盘
        verifyNotClosed();
        getEngine().syncTranslog();
    }

    /**
     * Checks if the underlying storage sync is required.
     */
    public boolean isSyncNeeded() {
        return getEngine().isTranslogSyncNeeded(); // NOTE: htt, 判断是否需要刷盘
    }

    /**
     * Returns the current translog durability mode
     */
    public Translog.Durability getTranslogDurability() {
        return indexSettings.getTranslogDurability(); // NOTE: htt, translog刷盘模式
    }

    // we can not protect with a lock since we "release" on a different thread
    private final AtomicBoolean flushOrRollRunning = new AtomicBoolean(); // NOTE: htt, 单个shard原子执行刷盘操作

    /**
     * Schedules a flush or translog generation roll if needed but will not schedule more than one concurrently. The operation will be
     * executed asynchronously on the flush thread pool.
     */
    public void afterWriteOperation() { // NOTE: htt, 写操作之后处理，包括 数据flush() 和 translog 滚动处理
        if (shouldPeriodicallyFlush() || shouldRollTranslogGeneration()) {
            if (flushOrRollRunning.compareAndSet(false, true)) {
                /*
                 * We have to check again since otherwise there is a race when a thread passes the first check next to another thread which
                 * performs the operation quickly enough to  finish before the current thread could flip the flag. In that situation, we
                 * have an extra operation.
                 *
                 * Additionally, a flush implicitly executes a translog generation roll so if we execute a flush then we do not need to
                 * check if we should roll the translog generation.
                 */
                if (shouldPeriodicallyFlush()) {
                    logger.debug("submitting async flush request");
                    final AbstractRunnable flush = new AbstractRunnable() {
                        @Override
                        public void onFailure(final Exception e) {
                            if (state != IndexShardState.CLOSED) {
                                logger.warn("failed to flush index", e);
                            }
                        }

                        @Override
                        protected void doRun() throws IOException {
                            flush(new FlushRequest()); // NOTE: htt, 执行flush刷盘操作
                            periodicFlushMetric.inc();
                        }

                        @Override
                        public void onAfter() {
                            flushOrRollRunning.compareAndSet(true, false); // NOTE: htt, 调整flush操作，单个shard一次只用一个flush
                            afterWriteOperation(); // NOTE: htt, 继续下一个flush或rollTranslog处理
                        }
                    };
                    threadPool.executor(ThreadPool.Names.FLUSH).execute(flush); // NOTE: htt, 异步执行flush
                } else if (shouldRollTranslogGeneration()) { // NOTE: htt, 如果需要滚动 translog
                    logger.debug("submitting async roll translog generation request");
                    final AbstractRunnable roll = new AbstractRunnable() {
                        @Override
                        public void onFailure(final Exception e) {
                            if (state != IndexShardState.CLOSED) {
                                logger.warn("failed to roll translog generation", e);
                            }
                        }

                        @Override
                        protected void doRun() throws Exception {
                            rollTranslogGeneration(); // NOTE: htt, 执行translog的滚动处理
                        }

                        @Override
                        public void onAfter() {
                            flushOrRollRunning.compareAndSet(true, false);  // NOTE: htt, 调整translog roll操作，单个shard一次只用一个roll
                            afterWriteOperation(); // NOTE: htt, 继续下一个flush或rollTranslog处理
                        }
                    };
                    threadPool.executor(ThreadPool.Names.FLUSH).execute(roll); // NTOE: htt, 异步执行translog的滚动
                } else {
                    flushOrRollRunning.compareAndSet(true, false);
                }
            }
        }
    }

    /**
     * Build {@linkplain RefreshListeners} for this shard.
     */
    private RefreshListeners buildRefreshListeners() { // NOTE: htt, 生成refresh监听，包括 段文件数据的refresh处理
        return new RefreshListeners(
            indexSettings::getMaxRefreshListeners,
            () -> refresh("too_many_listeners"), // NOTE: htt, 执行refresh处理
            threadPool.executor(ThreadPool.Names.LISTENER)::execute,
            logger, threadPool.getThreadContext());
    }

    /**
     * Simple struct encapsulating a shard failure
     *
     * @see IndexShard#addShardFailureCallback(Consumer)
     */
    public static final class ShardFailure { // NOTE: htt, shard失败
        public final ShardRouting routing;
        public final String reason;
        @Nullable
        public final Exception cause;

        public ShardFailure(ShardRouting routing, String reason, @Nullable Exception cause) {
            this.routing = routing;
            this.reason = reason;
            this.cause = cause;
        }
    }

    EngineFactory getEngineFactory() {
        return engineFactory;
    }

    // for tests
    ReplicationTracker getReplicationTracker() {
        return replicationTracker; // NOTE: htt, 复制组信息跟踪，包括每个副本的checkPoint（本地以及全局）
    }

    /**
     * Returns <code>true</code> iff one or more changes to the engine are not visible to via the current searcher *or* there are pending
     * refresh listeners.
     * Otherwise <code>false</code>.
     *
     * @throws AlreadyClosedException if the engine or internal indexwriter in the engine is already closed
     */
    public boolean isRefreshNeeded() {
        return getEngine().refreshNeeded() || (refreshListeners != null && refreshListeners.refreshNeeded());
    }

    /**
     * Add a listener for refreshes.
     *
     * @param location the location to listen for
     * @param listener for the refresh. Called with true if registering the listener ran it out of slots and forced a refresh. Called with
     *        false otherwise.
     */
    public void addRefreshListener(Translog.Location location, Consumer<Boolean> listener) {
        final boolean readAllowed;
        if (isReadAllowed()) { // NOTE: htt, 索引可以读，即POST_RECOVERY，或者STARTED
            readAllowed = true;
        } else {
            // check again under mutex. this is important to create a happens before relationship
            // between the switch to POST_RECOVERY + associated refresh. Otherwise we may respond
            // to a listener before a refresh actually happened that contained that operation.
            synchronized (mutex) {
                readAllowed = isReadAllowed();
            }
        }
        if (readAllowed) {
            refreshListeners.addOrNotify(location, listener); // NOTE: htt, 添加监听处理，
        } else {
            // we're not yet ready fo ready for reads, just ignore refresh cycles
            listener.accept(false);
        }
    }

    private static class RefreshMetricUpdater implements ReferenceManager.RefreshListener { // NOTE; htt, 记录refresh启动前后时间差，即refresh的时间

        private final MeanMetric refreshMetric;
        private long currentRefreshStartTime;
        private Thread callingThread = null;

        private RefreshMetricUpdater(MeanMetric refreshMetric) {
            this.refreshMetric = refreshMetric;
        }

        @Override
        public void beforeRefresh() throws IOException {
            if (Assertions.ENABLED) {
                assert callingThread == null : "beforeRefresh was called by " + callingThread.getName() +
                    " without a corresponding call to afterRefresh";
                callingThread = Thread.currentThread();
            }
            currentRefreshStartTime = System.nanoTime();
        }

        @Override
        public void afterRefresh(boolean didRefresh) throws IOException { // NOTE: htt, refresh之后，增加统计
            if (Assertions.ENABLED) {
                assert callingThread != null : "afterRefresh called but not beforeRefresh";
                assert callingThread == Thread.currentThread() : "beforeRefreshed called by a different thread. current ["
                    + Thread.currentThread().getName() + "], thread that called beforeRefresh [" + callingThread.getName() + "]";
                callingThread = null;
            }
            refreshMetric.inc(System.nanoTime() - currentRefreshStartTime);
        }
    }
}
