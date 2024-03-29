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

package org.elasticsearch.index.engine;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.CheckedRunnable;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.lucene.uid.VersionsAndSeqNoResolver;
import org.elasticsearch.common.lucene.uid.VersionsAndSeqNoResolver.DocIdAndVersion;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.ParseContext.Document;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.seqno.LocalCheckpointTracker;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogStats;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiFunction;
import java.util.stream.Stream;

public abstract class Engine implements Closeable { // NOTE: htt, 包括translog处理以及段文件处理，并包含Index/delete/get/search等操作和操作结果

    public static final String SYNC_COMMIT_ID = "sync_id"; // NOTE: htt, sync id, 即执行synced flush 会生成一个syncId,有相同syncId则代表有相同的lucene索引，主要用于冷索引，即长时间没有写入；目的是快速恢复冷数据；热分片快速恢复可以用global_checkPoint比对校验
    public static final String HISTORY_UUID_KEY = "history_uuid";

    protected final ShardId shardId;
    protected final String allocationId;
    protected final Logger logger;
    protected final EngineConfig engineConfig; // NOTE: htt, 引擎配置
    protected final Store store; // NOTE: htt, 用于访问shard，每个shard有对应实例，用于获取shard下的段文件的元信息，以及各个段文件的checksum等，并对段文件等进行校验
    protected final AtomicBoolean isClosed = new AtomicBoolean(false); // NOTE: htt, close 等待锁
    private final CountDownLatch closedLatch = new CountDownLatch(1); // NOTE: htt, close latch
    protected final EventListener eventListener; // NOTE: htt, onFailed operation
    protected final ReentrantLock failEngineLock = new ReentrantLock();
    protected final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock(); // NOTE: htt, 读写锁
    protected final ReleasableLock readLock = new ReleasableLock(rwl.readLock()); // NOTE: htt, 读锁
    protected final ReleasableLock writeLock = new ReleasableLock(rwl.writeLock()); // NOTE: htt, 写锁
    protected final SetOnce<Exception> failedEngine = new SetOnce<>();
    /*
     * on <tt>lastWriteNanos</tt> we use System.nanoTime() to initialize this since:
     *  - we use the value for figuring out if the shard / engine is active so if we startup and no write has happened yet we still consider it active
     *    for the duration of the configured active to inactive period. If we initialize to 0 or Long.MAX_VALUE we either immediately or never mark it
     *    inactive if no writes at all happen to the shard.
     *  - we also use this to flush big-ass merges on an inactive engine / shard but if we we initialize 0 or Long.MAX_VALUE we either immediately or never
     *    commit merges even though we shouldn't from a user perspective (this can also have funky sideeffects in tests when we open indices with lots of segments
     *    and suddenly merges kick in.
     *  NOTE: don't use this value for anything accurate it's a best effort for freeing up diskspace after merges and on a shard level to reduce index buffer sizes on
     *  inactive shards.
     */
    protected volatile long lastWriteNanos = System.nanoTime();

    protected Engine(EngineConfig engineConfig) {
        Objects.requireNonNull(engineConfig.getStore(), "Store must be provided to the engine");

        this.engineConfig = engineConfig;
        this.shardId = engineConfig.getShardId();
        this.allocationId = engineConfig.getAllocationId();
        this.store = engineConfig.getStore();
        this.logger = Loggers.getLogger(Engine.class, // we use the engine class directly here to make sure all subclasses have the same logger name
                engineConfig.getIndexSettings().getSettings(), engineConfig.getShardId());
        this.eventListener = engineConfig.getEventListener();
    }

    /** Returns 0 in the case where accountable is null, otherwise returns {@code ramBytesUsed()} */
    protected static long guardedRamBytesUsed(Accountable a) {
        if (a == null) {
            return 0;
        }
        return a.ramBytesUsed();
    }

    /**
     * Returns whether a leaf reader comes from a merge (versus flush or addIndexes).
     */
    protected static boolean isMergedSegment(LeafReader reader) { // NOTE: htt, 判断段是否有merge产生
        // We expect leaves to be segment readers
        final Map<String, String> diagnostics = Lucene.segmentReader(reader).getSegmentInfo().info.getDiagnostics();
        final String source = diagnostics.get(IndexWriter.SOURCE);
        assert Arrays.asList(IndexWriter.SOURCE_ADDINDEXES_READERS, IndexWriter.SOURCE_FLUSH,
                IndexWriter.SOURCE_MERGE).contains(source) : "Unknown source " + source;
        return IndexWriter.SOURCE_MERGE.equals(source);
    }

    public final EngineConfig config() {
        return engineConfig;
    }

    protected abstract SegmentInfos getLastCommittedSegmentInfos();

    public MergeStats getMergeStats() {
        return new MergeStats(); // NOTE: htt, 需要被覆盖，否则每次都生成空的 merge统计 信息
    }

    /** returns the history uuid for the engine */
    public abstract String getHistoryUUID();

    /** Returns how many bytes we are currently moving from heap to disk */
    public abstract long getWritingBytes();

    /**
     * A throttling class that can be activated, causing the
     * {@code acquireThrottle} method to block on a lock when throttling
     * is enabled
     */
    protected static final class IndexThrottle { // NOTE: htt, 索引限流判断，其中如果激活则采用真实锁
        private final CounterMetric throttleTimeMillisMetric = new CounterMetric();
        private volatile long startOfThrottleNS;
        private static final ReleasableLock NOOP_LOCK = new ReleasableLock(new NoOpLock());
        private final ReleasableLock lockReference = new ReleasableLock(new ReentrantLock());
        private volatile ReleasableLock lock = NOOP_LOCK;

        public Releasable acquireThrottle() {
            return lock.acquire();
        }

        /** Activate throttling, which switches the lock to be a real lock */
        public void activate() { // NOTE: htt, 激活限流，则将lock替换为 真实锁
            assert lock == NOOP_LOCK : "throttling activated while already active";
            startOfThrottleNS = System.nanoTime();
            lock = lockReference;
        }

        /** Deactivate throttling, which switches the lock to be an always-acquirable NoOpLock */
        public void deactivate() { // NOTE: htt, 关闭限流，则将lock替换为 noOpLock
            assert lock != NOOP_LOCK : "throttling deactivated but not active";
            lock = NOOP_LOCK;

            assert startOfThrottleNS > 0 : "Bad state of startOfThrottleNS";
            long throttleTimeNS = System.nanoTime() - startOfThrottleNS;
            if (throttleTimeNS >= 0) {
                // Paranoia (System.nanoTime() is supposed to be monotonic): time slip may have occurred but never want to add a negative number
                throttleTimeMillisMetric.inc(TimeValue.nsecToMSec(throttleTimeNS));
            }
        }

        long getThrottleTimeInMillis() {
            long currentThrottleNS = 0;
            if (isThrottled() && startOfThrottleNS != 0) {
                currentThrottleNS +=  System.nanoTime() - startOfThrottleNS;
                if (currentThrottleNS < 0) {
                    // Paranoia (System.nanoTime() is supposed to be monotonic): time slip must have happened, have to ignore this value
                    currentThrottleNS = 0;
                }
            }
            return throttleTimeMillisMetric.count() + TimeValue.nsecToMSec(currentThrottleNS);
        }

        boolean isThrottled() {
            return lock != NOOP_LOCK;
        }
    }

    /**
     * Returns the number of milliseconds this engine was under index throttling.
     */
    public abstract long getIndexThrottleTimeInMillis();

    /**
     * Returns the <code>true</code> iff this engine is currently under index throttling.
     * @see #getIndexThrottleTimeInMillis()
     */
    public abstract boolean isThrottled();

    /** A Lock implementation that always allows the lock to be acquired */
    protected static final class NoOpLock implements Lock { // NOTE: htt, no lock

        @Override
        public void lock() {
        }

        @Override
        public void lockInterruptibly() throws InterruptedException {
        }

        @Override
        public boolean tryLock() {
            return true;
        }

        @Override
        public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
            return true;
        }

        @Override
        public void unlock() {
        }

        @Override
        public Condition newCondition() {
            throw new UnsupportedOperationException("NoOpLock can't provide a condition");
        }
    }

    /**
     * Perform document index operation on the engine
     * @param index operation to perform
     * @return {@link IndexResult} containing updated translog location, version and
     * document specific failures
     *
     * Note: engine level failures (i.e. persistent engine failures) are thrown
     */
    public abstract IndexResult index(Index index) throws IOException;

    /**
     * Perform document delete operation on the engine
     * @param delete operation to perform
     * @return {@link DeleteResult} containing updated translog location, version and
     * document specific failures
     *
     * Note: engine level failures (i.e. persistent engine failures) are thrown
     */
    public abstract DeleteResult delete(Delete delete) throws IOException;

    public abstract NoOpResult noOp(NoOp noOp);

    /**
     * Base class for index and delete operation results
     * Holds result meta data (e.g. translog location, updated version)
     * for an executed write {@link Operation}
     **/
    public abstract static class Result { // NOTE: htt, base result for index/delete operation
        private final Operation.TYPE operationType; // NOTE: htt, 操作类型
        private final Result.Type resultType; // NOTE: htt, 结果类型，是否需要更新mapping，如果需要，则先更新mapping
        private final long version;
        private final long seqNo; // NOTE: htt, seqNo
        private final Exception failure; // NOTE: htt, 执行一次
        private final SetOnce<Boolean> freeze = new SetOnce<>();
        private final Mapping requiredMappingUpdate; // NOTE: htt, index mapping
        private Translog.Location translogLocation; // NOTE: htt, 写入后对应的translog的位置
        private long took; // NOTE: htt, 操作花费的时间

        protected Result(Operation.TYPE operationType, Exception failure, long version, long seqNo) {
            this.operationType = operationType;
            this.failure = Objects.requireNonNull(failure);
            this.version = version;
            this.seqNo = seqNo;
            this.requiredMappingUpdate = null;
            this.resultType = Type.FAILURE;
        }

        protected Result(Operation.TYPE operationType, long version, long seqNo) {
            this.operationType = operationType;
            this.version = version;
            this.seqNo = seqNo;
            this.failure = null;
            this.requiredMappingUpdate = null;
            this.resultType = Type.SUCCESS;
        }

        protected Result(Operation.TYPE operationType, Mapping requiredMappingUpdate) {
            this.operationType = operationType;
            this.version = Versions.NOT_FOUND;
            this.seqNo = SequenceNumbers.UNASSIGNED_SEQ_NO;
            this.failure = null;
            this.requiredMappingUpdate = requiredMappingUpdate;
            this.resultType = Type.MAPPING_UPDATE_REQUIRED;
        }

        /** whether the operation was successful, has failed or was aborted due to a mapping update */
        public Type getResultType() {
            return resultType;
        }

        /** get the updated document version */
        public long getVersion() {
            return version;
        }

        /**
         * Get the sequence number on the primary.
         *
         * @return the sequence number
         */
        public long getSeqNo() {
            return seqNo;
        }

        /**
         * If the operation was aborted due to missing mappings, this method will return the mappings
         * that are required to complete the operation.
         */
        public Mapping getRequiredMappingUpdate() {
            return requiredMappingUpdate;
        }

        /** get the translog location after executing the operation */
        public Translog.Location getTranslogLocation() {
            return translogLocation;
        }

        /** get document failure while executing the operation {@code null} in case of no failure */
        public Exception getFailure() {
            return failure;
        }

        /** get total time in nanoseconds */
        public long getTook() {
            return took; // NOTE: htt, 数据写入花费的时间
        }

        public Operation.TYPE getOperationType() {
            return operationType;
        }

        void setTranslogLocation(Translog.Location translogLocation) {
            if (freeze.get() == null) {
                this.translogLocation = translogLocation;
            } else {
                throw new IllegalStateException("result is already frozen");
            }
        }

        void setTook(long took) {
            if (freeze.get() == null) {
                this.took = took;
            } else {
                throw new IllegalStateException("result is already frozen");
            }
        }

        void freeze() {
            freeze.set(true);
        }

        public enum Type { // NOTE: htt, result type
            SUCCESS,
            FAILURE,
            MAPPING_UPDATE_REQUIRED // NOTE: htt, mapping更新请求
        }
    }

    public static class IndexResult extends Result { // NOTE: htt, index operation result including created type

        private final boolean created; // NOTE: htt, 是否创建成功

        public IndexResult(long version, long seqNo, boolean created) {
            super(Operation.TYPE.INDEX, version, seqNo);
            this.created = created;
        }

        /**
         * use in case of the index operation failed before getting to internal engine
         **/
        public IndexResult(Exception failure, long version) {
            this(failure, version, SequenceNumbers.UNASSIGNED_SEQ_NO);
        }

        public IndexResult(Exception failure, long version, long seqNo) {
            super(Operation.TYPE.INDEX, failure, version, seqNo);
            this.created = false;
        }

        public IndexResult(Mapping requiredMappingUpdate) {
            super(Operation.TYPE.INDEX, requiredMappingUpdate);
            this.created = false;
        }

        public boolean isCreated() {
            return created;
        }

    }

    public static class DeleteResult extends Result { // NOTE: htt, delete result including whether found

        private final boolean found; // NOTE: htt, 是否找到

        public DeleteResult(long version, long seqNo, boolean found) {
            super(Operation.TYPE.DELETE, version, seqNo);
            this.found = found;
        }

        /**
         * use in case of the delete operation failed before getting to internal engine
         **/
        public DeleteResult(Exception failure, long version) {
            this(failure, version, SequenceNumbers.UNASSIGNED_SEQ_NO, false);
        }

        public DeleteResult(Exception failure, long version, long seqNo, boolean found) {
            super(Operation.TYPE.DELETE, failure, version, seqNo);
            this.found = found;
        }

        public DeleteResult(Mapping requiredMappingUpdate) {
            super(Operation.TYPE.DELETE, requiredMappingUpdate);
            this.found = false;
        }

        public boolean isFound() {
            return found;
        }

    }

    public static class NoOpResult extends Result { // NOTE: htt, no op result

        NoOpResult(long seqNo) {
            super(Operation.TYPE.NO_OP, 0, seqNo);
        }

        NoOpResult(long seqNo, Exception failure) {
            super(Operation.TYPE.NO_OP, failure, 0, seqNo);
        }

    }

    /**
     * Attempts to do a special commit where the given syncID is put into the commit data. The attempt
     * succeeds if there are not pending writes in lucene and the current point is equal to the expected one.
     *
     * @param syncId           id of this sync
     * @param expectedCommitId the expected value of
     * @return true if the sync commit was made, false o.w.
     */
    public abstract SyncedFlushResult syncFlush(String syncId, CommitId expectedCommitId) throws EngineException;

    public enum SyncedFlushResult { // NOTE: htt, syncedFlush 结果
        SUCCESS,
        COMMIT_MISMATCH,
        PENDING_OPERATIONS
    }

    protected final GetResult getFromSearcher(Get get, BiFunction<String, SearcherScope, Searcher> searcherFactory,
                                              SearcherScope scope) throws EngineException { // NOTE: htt, 从searcher获取结果
        final Searcher searcher = searcherFactory.apply("get", scope);
        final DocIdAndVersion docIdAndVersion;
        try {
            docIdAndVersion = VersionsAndSeqNoResolver.loadDocIdAndVersion(searcher.reader(), get.uid());
        } catch (Exception e) {
            Releasables.closeWhileHandlingException(searcher);
            //TODO: A better exception goes here
            throw new EngineException(shardId, "Couldn't resolve version", e);
        }

        if (docIdAndVersion != null) {
            if (get.versionType().isVersionConflictForReads(docIdAndVersion.version, get.version())) { // NOTE: htt, 如果version出现冲突则关闭
                Releasables.close(searcher);
                throw new VersionConflictEngineException(shardId, get.type(), get.id(),
                        get.versionType().explainConflictForReads(docIdAndVersion.version, get.version()));
            }
        }

        if (docIdAndVersion != null) {
            // don't release the searcher on this path, it is the
            // responsibility of the caller to call GetResult.release
            return new GetResult(searcher, docIdAndVersion);
        } else {
            Releasables.close(searcher);
            return GetResult.NOT_EXISTS;
        }
    }

    public abstract GetResult get(Get get, BiFunction<String, SearcherScope, Searcher> searcherFactory) throws EngineException;


    /**
     * Returns a new searcher instance. The consumer of this
     * API is responsible for releasing the returned searcher in a
     * safe manner, preferably in a try/finally block.
     *
     * @param source the source API or routing that triggers this searcher acquire
     *
     * @see Searcher#close()
     */
    public final Searcher acquireSearcher(String source) throws EngineException {
        return acquireSearcher(source, SearcherScope.EXTERNAL);
    }

    /**
     * Returns a new searcher instance. The consumer of this
     * API is responsible for releasing the returned searcher in a
     * safe manner, preferably in a try/finally block.
     *
     * @param source the source API or routing that triggers this searcher acquire
     * @param scope the scope of this searcher ie. if the searcher will be used for get or search purposes
     *
     * @see Searcher#close()
     */
    public abstract Searcher acquireSearcher(String source, SearcherScope scope) throws EngineException;

    public enum SearcherScope { // NOTE: htt, 内部和外部
        EXTERNAL, INTERNAL
    }

    /**
     * Returns the translog associated with this engine.
     * Prefer to keep the translog package-private, so that an engine can control all accesses to the translog.
     */
    abstract Translog getTranslog();

    /**
     * Checks if the underlying storage sync is required.
     */
    public boolean isTranslogSyncNeeded() {
        return getTranslog().syncNeeded();
    }

    /**
     * Ensures that all locations in the given stream have been written to the underlying storage.
     */
    public abstract boolean ensureTranslogSynced(Stream<Translog.Location> locations) throws IOException;

    public abstract void syncTranslog() throws IOException;

    public Closeable acquireTranslogRetentionLock() {
        return getTranslog().acquireRetentionLock();
    }

    /**
     * Creates a new translog snapshot from this engine for reading translog operations whose seq# at least the provided seq#.
     * The caller has to close the returned snapshot after finishing the reading.
     */
    public Translog.Snapshot newTranslogSnapshotFromMinSeqNo(long minSeqNo) throws IOException {
        return getTranslog().newSnapshotFromMinSeqNo(minSeqNo);
    }

    /**
     * Returns the estimated number of translog operations in this engine whose seq# at least the provided seq#.
     */
    public int estimateTranslogOperationsFromMinSeq(long minSeqNo) {
        return getTranslog().estimateTotalOperationsFromMinSeq(minSeqNo);
    }

    public TranslogStats getTranslogStats() {
        return getTranslog().stats();  // NOTE: htt, translog统计信息
    }

    /**
     * Returns the last location that the translog of this engine has written into.
     */
    public Translog.Location getTranslogLastWriteLocation() {
        return getTranslog().getLastWriteLocation();
    }

    protected final void ensureOpen(Exception suppressed) {
        if (isClosed.get()) {
            AlreadyClosedException ace = new AlreadyClosedException(shardId + " engine is closed", failedEngine.get());
            if (suppressed != null) {
                ace.addSuppressed(suppressed);
            }
            throw ace;
        }
    }

    protected final void ensureOpen() {
        ensureOpen(null);
    }

    /** get commits stats for the last commit */
    public CommitStats commitStats() {
        return new CommitStats(getLastCommittedSegmentInfos());
    }

    /**
     * The sequence number service for this engine.
     *
     * @return the sequence number service
     */
    public abstract LocalCheckpointTracker getLocalCheckpointTracker(); // NOTE: htt, 获得本地 checkPoint 类

    /**
     * Returns the latest global checkpoint value that has been persisted in the underlying storage (i.e. translog's checkpoint)
     */
    public long getLastSyncedGlobalCheckpoint() {
        return getTranslog().getLastSyncedGlobalCheckpoint(); // NOTE: htt, 获取当前最新持久化的global checkpoint
    }

    /**
     * Global stats on segments.
     */
    public final SegmentsStats segmentsStats(boolean includeSegmentFileSizes) { // NOTE: htt, 获取段内全局统计信息
        ensureOpen();
        Set<String> segmentName = new HashSet<>();
        SegmentsStats stats = new SegmentsStats();
        try (Searcher searcher = acquireSearcher("segments_stats", SearcherScope.INTERNAL)) {
            for (LeafReaderContext ctx : searcher.reader().getContext().leaves()) {
                SegmentReader segmentReader = Lucene.segmentReader(ctx.reader());
                fillSegmentStats(segmentReader, includeSegmentFileSizes, stats);
                segmentName.add(segmentReader.getSegmentName());
            }
        }

        try (Searcher searcher = acquireSearcher("segments_stats", SearcherScope.EXTERNAL)) {
            for (LeafReaderContext ctx : searcher.reader().getContext().leaves()) {
                SegmentReader segmentReader = Lucene.segmentReader(ctx.reader());
                if (segmentName.contains(segmentReader.getSegmentName()) == false) { // NOTE: htt, 如果当前段不包含该文件
                    fillSegmentStats(segmentReader, includeSegmentFileSizes, stats);
                }
            }
        }
        writerSegmentStats(stats);
        return stats;
    }

    private void fillSegmentStats(SegmentReader segmentReader, boolean includeSegmentFileSizes, SegmentsStats stats) {
        stats.add(1, segmentReader.ramBytesUsed());
        stats.addTermsMemoryInBytes(guardedRamBytesUsed(segmentReader.getPostingsReader()));
        stats.addStoredFieldsMemoryInBytes(guardedRamBytesUsed(segmentReader.getFieldsReader()));
        stats.addTermVectorsMemoryInBytes(guardedRamBytesUsed(segmentReader.getTermVectorsReader()));
        stats.addNormsMemoryInBytes(guardedRamBytesUsed(segmentReader.getNormsReader()));
        stats.addPointsMemoryInBytes(guardedRamBytesUsed(segmentReader.getPointsReader()));
        stats.addDocValuesMemoryInBytes(guardedRamBytesUsed(segmentReader.getDocValuesReader()));

        if (includeSegmentFileSizes) {
            // TODO: consider moving this to StoreStats
            stats.addFileSizes(getSegmentFileSizes(segmentReader));
        }
    }

    private ImmutableOpenMap<String, Long> getSegmentFileSizes(SegmentReader segmentReader) { // NOTE: htt, 获取段内每种文件的长度
        Directory directory = null;
        SegmentCommitInfo segmentCommitInfo = segmentReader.getSegmentInfo();
        boolean useCompoundFile = segmentCommitInfo.info.getUseCompoundFile();
        if (useCompoundFile) {
            try {
                directory = engineConfig.getCodec().compoundFormat().getCompoundReader(segmentReader.directory(), segmentCommitInfo.info, IOContext.READ);
            } catch (IOException e) {
                logger.warn(() -> new ParameterizedMessage("Error when opening compound reader for Directory [{}] and SegmentCommitInfo [{}]", segmentReader.directory(), segmentCommitInfo), e);

                return ImmutableOpenMap.of();
            }
        } else {
            directory = segmentReader.directory();
        }

        assert directory != null;

        String[] files;
        if (useCompoundFile) {
            try {
                files = directory.listAll();
            } catch (IOException e) {
                final Directory finalDirectory = directory;
                logger.warn(() -> new ParameterizedMessage("Couldn't list Compound Reader Directory [{}]", finalDirectory), e);
                return ImmutableOpenMap.of();
            }
        } else {
            try {
                files = segmentReader.getSegmentInfo().files().toArray(new String[]{}); // NOTE: htt, 获取所有段下文件
            } catch (IOException e) {
                logger.warn(() -> new ParameterizedMessage("Couldn't list Directory from SegmentReader [{}] and SegmentInfo [{}]", segmentReader, segmentReader.getSegmentInfo()), e);
                return ImmutableOpenMap.of();
            }
        }

        ImmutableOpenMap.Builder<String, Long> map = ImmutableOpenMap.builder();
        for (String file : files) {
            String extension = IndexFileNames.getExtension(file);
            long length = 0L;
            try {
                length = directory.fileLength(file);
            } catch (NoSuchFileException | FileNotFoundException e) {
                final Directory finalDirectory = directory;
                logger.warn(() -> new ParameterizedMessage("Tried to query fileLength but file is gone [{}] [{}]", finalDirectory, file), e);
            } catch (IOException e) {
                final Directory finalDirectory = directory;
                logger.warn(() -> new ParameterizedMessage("Error when trying to query fileLength [{}] [{}]", finalDirectory, file), e);
            }
            if (length == 0L) {
                continue;
            }
            map.put(extension, length); // NOTE: 每种后缀文件的长度， 如 .dvd -> 100B
        }

        if (useCompoundFile && directory != null) {
            try {
                directory.close();
            } catch (IOException e) {
                final Directory finalDirectory = directory;
                logger.warn(() -> new ParameterizedMessage("Error when closing compound reader on Directory [{}]", finalDirectory), e);
            }
        }

        return map.build();
    }

    protected void writerSegmentStats(SegmentsStats stats) {
        // by default we don't have a writer here... subclasses can override this
        stats.addVersionMapMemoryInBytes(0);
        stats.addIndexWriterMemoryInBytes(0);
    }

    /** How much heap is used that would be freed by a refresh.  Note that this may throw {@link AlreadyClosedException}. */
    public abstract long getIndexBufferRAMBytesUsed();

    protected Segment[] getSegmentInfo(SegmentInfos lastCommittedSegmentInfos, boolean verbose) { // NOTE: htt, 获取各个段信息
        ensureOpen();
        Map<String, Segment> segments = new HashMap<>();
        // first, go over and compute the search ones...
        try (Searcher searcher = acquireSearcher("segments", SearcherScope.EXTERNAL)){
            for (LeafReaderContext ctx : searcher.reader().getContext().leaves()) {
                fillSegmentInfo(Lucene.segmentReader(ctx.reader()), verbose, true, segments);
            }
        }

        try (Searcher searcher = acquireSearcher("segments", SearcherScope.INTERNAL)){
            for (LeafReaderContext ctx : searcher.reader().getContext().leaves()) {
                SegmentReader segmentReader = Lucene.segmentReader(ctx.reader());
                if (segments.containsKey(segmentReader.getSegmentName()) == false) { // NOTE: htt, 当前未包含
                    fillSegmentInfo(segmentReader, verbose, false, segments);
                }
            }
        }

        // now, correlate or add the committed ones...
        if (lastCommittedSegmentInfos != null) {
            SegmentInfos infos = lastCommittedSegmentInfos;
            for (SegmentCommitInfo info : infos) {
                Segment segment = segments.get(info.info.name);
                if (segment == null) {
                    segment = new Segment(info.info.name);
                    segment.search = false;
                    segment.committed = true;
                    segment.docCount = info.info.maxDoc();
                    segment.delDocCount = info.getDelCount();
                    segment.version = info.info.getVersion();
                    segment.compound = info.info.getUseCompoundFile();
                    try {
                        segment.sizeInBytes = info.sizeInBytes();
                    } catch (IOException e) {
                        logger.trace(() -> new ParameterizedMessage("failed to get size for [{}]", info.info.name), e);
                    }
                    segments.put(info.info.name, segment);
                } else {
                    segment.committed = true;
                }
            }
        }

        Segment[] segmentsArr = segments.values().toArray(new Segment[segments.values().size()]);
        Arrays.sort(segmentsArr, Comparator.comparingLong(Segment::getGeneration)); // NOTE: htt，按段的 generation进行排序
        return segmentsArr;
    }

    private void fillSegmentInfo(SegmentReader segmentReader, boolean verbose, boolean search, Map<String, Segment> segments) {
        SegmentCommitInfo info = segmentReader.getSegmentInfo();
        assert segments.containsKey(info.info.name) == false;
        Segment segment = new Segment(info.info.name);
        segment.search = search;
        segment.docCount = segmentReader.numDocs();
        segment.delDocCount = segmentReader.numDeletedDocs();
        segment.version = info.info.getVersion();
        segment.compound = info.info.getUseCompoundFile();
        try {
            segment.sizeInBytes = info.sizeInBytes();
        } catch (IOException e) {
            logger.trace(() -> new ParameterizedMessage("failed to get size for [{}]", info.info.name), e);
        }
        segment.memoryInBytes = segmentReader.ramBytesUsed();
        segment.segmentSort = info.info.getIndexSort();
        if (verbose) {
            segment.ramTree = Accountables.namedAccountable("root", segmentReader);
        }
        segment.attributes = info.info.getAttributes();
        // TODO: add more fine grained mem stats values to per segment info here
        segments.put(info.info.name, segment);
    }

    /**
     * The list of segments in the engine.
     */
    public abstract List<Segment> segments(boolean verbose);

    public final boolean refreshNeeded() {
        if (store.tryIncRef()) {
            /*
              we need to inc the store here since we acquire a searcher and that might keep a file open on the
              store. this violates the assumption that all files are closed when
              the store is closed so we need to make sure we increment it here
             */
            try {
                try (Searcher searcher = acquireSearcher("refresh_needed", SearcherScope.EXTERNAL)) {
                    return searcher.getDirectoryReader().isCurrent() == false;
                }
            } catch (IOException e) {
                logger.error("failed to access searcher manager", e);
                failEngine("failed to access searcher manager", e);
                throw new EngineException(shardId, "failed to access searcher manager", e);
            } finally {
                store.decRef();
            }
        }
        return false;
    }

    /**
     * Synchronously refreshes the engine for new search operations to reflect the latest
     * changes.
     */
    @Nullable
    public abstract void refresh(String source) throws EngineException; // NOTE: htt, refresh shard文件，将对应段刷入到高缓，但不保证一定刷盘，此时可以进行读取并进行search

    /**
     * Called when our engine is using too much heap and should move buffered indexed/deleted documents to disk.
     */
    // NOTE: do NOT rename this to something containing flush or refresh!
    public abstract void writeIndexingBuffer() throws EngineException;

    /**
     * Checks if this engine should be flushed periodically.
     * This check is mainly based on the uncommitted translog size and the translog flush threshold setting.
     */
    public abstract boolean shouldPeriodicallyFlush();

    /**
     * Flushes the state of the engine including the transaction log, clearing memory.
     *
     * @param force         if <code>true</code> a lucene commit is executed even if no changes need to be committed.
     * @param waitIfOngoing if <code>true</code> this call will block until all currently running flushes have finished.
     *                      Otherwise this call will return without blocking.
     * @return the commit Id for the resulting commit
     */
    public abstract CommitId flush(boolean force, boolean waitIfOngoing) throws EngineException; // NOTE: htt, flush 会将数据刷盘

    /**
     * Flushes the state of the engine including the transaction log, clearing memory and persisting
     * documents in the lucene index to disk including a potentially heavy and durable fsync operation.
     * This operation is not going to block if another flush operation is currently running and won't write
     * a lucene commit if nothing needs to be committed.
     *
     * @return the commit Id for the resulting commit
     */
    public abstract CommitId flush() throws EngineException; // NOTE: htt, 强制刷盘，包括translog


    /**
     * checks and removes translog files that no longer need to be retained. See
     * {@link org.elasticsearch.index.translog.TranslogDeletionPolicy} for details
     */
    public abstract void trimTranslog() throws EngineException;

    /**
     * Tests whether or not the translog generation should be rolled to a new generation.
     * This test is based on the size of the current generation compared to the configured generation threshold size.
     *
     * @return {@code true} if the current generation should be rolled to a new generation
     */
    public boolean shouldRollTranslogGeneration() { // NOTE: 判断生成新的translog文件
        return getTranslog().shouldRollGeneration();
    }

    /**
     * Rolls the translog generation and cleans unneeded.
     */
    public abstract void rollTranslogGeneration() throws EngineException;

    /**
     * Force merges to 1 segment
     */
    public void forceMerge(boolean flush) throws IOException { // NOTE: htt, merge段文件，并且制定是否强制刷盘
        forceMerge(flush, 1, false, false, false);
    }

    /**
     * Triggers a forced merge on this engine
     */
    public abstract void forceMerge(boolean flush, int maxNumSegments, boolean onlyExpungeDeletes, boolean upgrade, boolean upgradeOnlyAncientSegments) throws EngineException, IOException;

    /**
     * Snapshots the most recent index and returns a handle to it. If needed will try and "commit" the
     * lucene index to make sure we have a "fresh" copy of the files to snapshot.
     *
     * @param flushFirst indicates whether the engine should flush before returning the snapshot
     */
    public abstract IndexCommitRef acquireLastIndexCommit(boolean flushFirst) throws EngineException;

    /**
     * Snapshots the most recent safe index commit from the engine.
     */
    public abstract IndexCommitRef acquireSafeIndexCommit() throws EngineException;

    /**
     * If the specified throwable contains a fatal error in the throwable graph, such a fatal error will be thrown. Callers should ensure
     * that there are no catch statements that would catch an error in the stack as the fatal error here should go uncaught and be handled
     * by the uncaught exception handler that we install during bootstrap. If the specified throwable does indeed contain a fatal error, the
     * specified message will attempt to be logged before throwing the fatal error. If the specified throwable does not contain a fatal
     * error, this method is a no-op.
     *
     * @param maybeMessage the message to maybe log
     * @param maybeFatal   the throwable that maybe contains a fatal error
     */
    @SuppressWarnings("finally")
    private void maybeDie(final String maybeMessage, final Throwable maybeFatal) {
        ExceptionsHelper.maybeError(maybeFatal, logger).ifPresent(error -> {
            try {
                logger.error(maybeMessage, error);
            } finally {
                throw error;
            }
        });
    }

    /**
     * fail engine due to some error. the engine will also be closed.
     * The underlying store is marked corrupted iff failure is caused by index corruption
     */
    public void failEngine(String reason, @Nullable Exception failure) { // NOTE: htt, 引擎执行失败
        if (failure != null) {
            maybeDie(reason, failure);
        }
        if (failEngineLock.tryLock()) {
            store.incRef();
            try {
                if (failedEngine.get() != null) {
                    logger.warn(() -> new ParameterizedMessage("tried to fail engine but engine is already failed. ignoring. [{}]", reason), failure);
                    return;
                }
                // this must happen before we close IW or Translog such that we can check this state to opt out of failing the engine
                // again on any caught AlreadyClosedException
                failedEngine.set((failure != null) ? failure : new IllegalStateException(reason));
                try {
                    // we just go and close this engine - no way to recover
                    closeNoLock("engine failed on: [" + reason + "]", closedLatch); // NOTE: htt, 关闭index shard索引
                } finally {
                    logger.warn(() -> new ParameterizedMessage("failed engine [{}]", reason), failure);
                    // we must set a failure exception, generate one if not supplied
                    // we first mark the store as corrupted before we notify any listeners
                    // this must happen first otherwise we might try to reallocate so quickly
                    // on the same node that we don't see the corrupted marker file when
                    // the shard is initializing
                    if (Lucene.isCorruptionException(failure)) {
                        try {  // NOTE: htt, 生成 corrupted文件，记录当前的exception信息
                            store.markStoreCorrupted(new IOException("failed engine (reason: [" + reason + "])", ExceptionsHelper.unwrapCorruption(failure)));
                        } catch (IOException e) {
                            logger.warn("Couldn't mark store corrupted", e);
                        }
                    }
                    eventListener.onFailedEngine(reason, failure);
                }
            } catch (Exception inner) {
                if (failure != null) inner.addSuppressed(failure);
                // don't bubble up these exceptions up
                logger.warn("failEngine threw exception", inner);
            } finally {
                store.decRef();
            }
        } else {
            logger.debug(() -> new ParameterizedMessage("tried to fail engine but could not acquire lock - engine should be failed by now [{}]", reason), failure);
        }
    }

    /** Check whether the engine should be failed */
    protected boolean maybeFailEngine(String source, Exception e) {
        if (Lucene.isCorruptionException(e)) { // NOTE: htt, 判断是否corrupted异常
            failEngine("corrupt file (source: [" + source + "])", e);
            return true;
        }
        return false;
    }


    public interface EventListener { // NOTE: htt, onFailed operation
        /**
         * Called when a fatal exception occurred
         */
        default void onFailedEngine(String reason, @Nullable Exception e) {
        }
    }

    public static class Searcher implements Releasable { // NOTE: htt, searcher which get indexReader

        private final String source; // NOTE: htt, 搜索的source源数据
        private final IndexSearcher searcher; // TODO: htt, indexSearcher

        public Searcher(String source, IndexSearcher searcher) {
            this.source = source;
            this.searcher = searcher;
        }

        /**
         * The source that caused this searcher to be acquired.
         */
        public String source() {
            return source;
        }

        public IndexReader reader() {
            return searcher.getIndexReader();
        }

        public DirectoryReader getDirectoryReader() {
            if (reader() instanceof DirectoryReader) {
                return (DirectoryReader) reader();
            }
            throw new IllegalStateException("Can't use " + reader().getClass() + " as a directory reader");
        }

        public IndexSearcher searcher() {
            return searcher;
        }

        @Override
        public void close() {
            // Nothing to close here
        }
    }

    public abstract static class Operation { // NOTE: htt, operation including index/delete

        /** type of operation (index, delete), subclasses use static types */
        public enum TYPE { // NOTE: htt, 操作类型
            INDEX, DELETE, NO_OP;

            private final String lowercase;

            TYPE() {
                this.lowercase = this.toString().toLowerCase(Locale.ROOT);
            }

            public String getLowercase() {
                return lowercase;
            }
        }

        private final Term uid; // NOTE: htt, 存储的_id的值， {_id, Uid.encodeId(doc.id()) }
        private final long version; // NOTE: htt, 版本号
        private final long seqNo; // NOTE: htt, seq no， 每个文档
        private final long primaryTerm; // NOTE: htt, primary term，当前shard共用，新选shard时会变化
        private final VersionType versionType; // NOTE: htt, 版本判断类型
        private final Origin origin; // NOTE:htt, 操作来源
        private final long startTime;

        public Operation(Term uid, long seqNo, long primaryTerm, long version, VersionType versionType, Origin origin, long startTime) {
            this.uid = uid;
            this.seqNo = seqNo;
            this.primaryTerm = primaryTerm;
            this.version = version;
            this.versionType = versionType;
            this.origin = origin;
            this.startTime = startTime;
        }

        public enum Origin { // NOTE: htt, Origin
            PRIMARY, // NOTE:htt, 写主
            REPLICA, // NOTE: htt, 副本请求
            PEER_RECOVERY,
            LOCAL_TRANSLOG_RECOVERY; // NOTE: htt, 本地translog恢复，此时在数据写入lucene后不用再记录translog

            public boolean isRecovery() {
                return this == PEER_RECOVERY || this == LOCAL_TRANSLOG_RECOVERY;
            }
        }

        public Origin origin() {
            return this.origin;
        }

        public Term uid() {
            return this.uid;
        }

        public long version() {
            return this.version;
        }

        public long seqNo() {
            return seqNo;
        }

        public long primaryTerm() {
            return primaryTerm;
        }

        public abstract int estimatedSizeInBytes();

        public VersionType versionType() {
            return this.versionType;
        }

        /**
         * Returns operation start time in nanoseconds.
         */
        public long startTime() {
            return this.startTime;
        }

        public abstract String type();

        abstract String id();

        public abstract TYPE operationType();
    }

    public static class Index extends Operation { // NOTE: htt, index operation，写入操作

        private final ParsedDocument doc; // NOTE: htt, 解析文档，这里是从source中解析出来
        private final long autoGeneratedIdTimestamp;
        private final boolean isRetry;

        public Index(Term uid, ParsedDocument doc, long seqNo, long primaryTerm, long version, VersionType versionType, Origin origin,
                     long startTime, long autoGeneratedIdTimestamp, boolean isRetry) {
            super(uid, seqNo, primaryTerm, version, versionType, origin, startTime);
            this.doc = doc;
            this.isRetry = isRetry;
            this.autoGeneratedIdTimestamp = autoGeneratedIdTimestamp;
        }

        public Index(Term uid, long primaryTerm, ParsedDocument doc) {
            this(uid, primaryTerm, doc, Versions.MATCH_ANY);
        } // TEST ONLY

        Index(Term uid, long primaryTerm, ParsedDocument doc, long version) {
            this(uid, doc, SequenceNumbers.UNASSIGNED_SEQ_NO, primaryTerm, version, VersionType.INTERNAL,
                Origin.PRIMARY, System.nanoTime(), -1, false);
        } // TEST ONLY

        public ParsedDocument parsedDoc() {
            return this.doc;
        }

        @Override
        public String type() {
            return this.doc.type();
        }

        @Override
        public String id() {
            return this.doc.id();
        }

        @Override
        public TYPE operationType() {
            return TYPE.INDEX;
        }

        public String routing() {
            return this.doc.routing();
        }

        public String parent() {
            return this.doc.parent();
        }

        public List<Document> docs() {
            return this.doc.docs();
        }

        public BytesReference source() {
            return this.doc.source();
        }

        @Override
        public int estimatedSizeInBytes() { // NOTE: htt, 评估记录大小
            return (id().length() + type().length()) * 2 + source().length() + 12;
        }

        /**
         * Returns a positive timestamp if the ID of this document is auto-generated by elasticsearch.
         * if this property is non-negative indexing code might optimize the addition of this document
         * due to it's append only nature.
         */
        public long getAutoGeneratedIdTimestamp() {
            return autoGeneratedIdTimestamp;
        }

        /**
         * Returns <code>true</code> if this index requests has been retried on the coordinating node and can therefor be delivered
         * multiple times. Note: this might also be set to true if an equivalent event occurred like the replay of the transaction log
         */
        public boolean isRetry() {
            return isRetry;
        }

    }

    public static class Delete extends Operation { // NOTE: htt, delete operation include type and id

        private final String type; // NOTE: htt, _doc
        private final String id; // NOTE: htt, id

        public Delete(String type, String id, Term uid, long seqNo, long primaryTerm, long version, VersionType versionType,
                      Origin origin, long startTime) {
            super(uid, seqNo, primaryTerm, version, versionType, origin, startTime);
            this.type = Objects.requireNonNull(type);
            this.id = Objects.requireNonNull(id);
        }

        public Delete(String type, String id, Term uid, long primaryTerm) {
            this(type, id, uid, SequenceNumbers.UNASSIGNED_SEQ_NO, primaryTerm, Versions.MATCH_ANY, VersionType.INTERNAL, Origin.PRIMARY, System.nanoTime());
        }

        public Delete(Delete template, VersionType versionType) {
            this(template.type(), template.id(), template.uid(), template.seqNo(), template.primaryTerm(), template.version(),
                    versionType, template.origin(), template.startTime());
        }

        @Override
        public String type() {
            return this.type;
        }

        @Override
        public String id() {
            return this.id;
        }

        @Override
        public TYPE operationType() {
            return TYPE.DELETE;
        }

        @Override
        public int estimatedSizeInBytes() {
            return (uid().field().length() + uid().text().length()) * 2 + 20;
        }

    }

    public static class NoOp extends Operation { // NOTE: htt, no operation

        private final String reason;

        public String reason() {
            return reason;
        }

        public NoOp(final long seqNo, final long primaryTerm, final Origin origin, final long startTime, final String reason) {
            super(null, seqNo, primaryTerm, Versions.NOT_FOUND, null, origin, startTime);
            this.reason = reason;
        }

        @Override
        public Term uid() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String type() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long version() {
            throw new UnsupportedOperationException();
        }

        @Override
        public VersionType versionType() {
            throw new UnsupportedOperationException();
        }

        @Override
        String id() {
            throw new UnsupportedOperationException();
        }

        @Override
        public TYPE operationType() {
            return TYPE.NO_OP;
        }

        @Override
        public int estimatedSizeInBytes() {
            return 2 * reason.length() + 2 * Long.BYTES;
        }

    }

    public static class Get { // NOTE; htt, get operation include uid/type
        private final boolean realtime; // NOTE: htt, 默认为实时
        private final Term uid; // NOTE: htt, id
        private final String type, id; // NOTE: htt, type 和 文档id
        private final boolean readFromTranslog; // NOTE: htt, 是否从 translog读取
        private long version = Versions.MATCH_ANY;
        private VersionType versionType = VersionType.INTERNAL;

        public Get(boolean realtime, boolean readFromTranslog, String type, String id, Term uid) {
            this.realtime = realtime;
            this.type = type;
            this.id = id;
            this.uid = uid;
            this.readFromTranslog = readFromTranslog;
        }

        public boolean realtime() {
            return this.realtime;
        }

        public String type() {
            return type;
        }

        public String id() {
            return id;
        }

        public Term uid() {
            return uid;
        }

        public long version() {
            return version;
        }

        public Get version(long version) {
            this.version = version;
            return this;
        }

        public VersionType versionType() {
            return versionType;
        }

        public Get versionType(VersionType versionType) {
            this.versionType = versionType;
            return this;
        }

        public boolean isReadFromTranslog() {
            return readFromTranslog;
        }
    }

    public static class GetResult implements Releasable { // NOTE: htt, get结果，包括docId/version，以及搜索结果
        private final boolean exists; // NOTE: htt, 是否存在
        private final long version; // NOTE: htt, 文档 version
        private final DocIdAndVersion docIdAndVersion;  // NOTE: htt, lucene docId and 文档version
        private final Searcher searcher; // NOTE: htt,

        public static final GetResult NOT_EXISTS = new GetResult(false, Versions.NOT_FOUND, null, null);

        private GetResult(boolean exists, long version, DocIdAndVersion docIdAndVersion, Searcher searcher) {
            this.exists = exists;
            this.version = version;
            this.docIdAndVersion = docIdAndVersion;
            this.searcher = searcher;
        }

        /**
         * Build a non-realtime get result from the searcher.
         */
        public GetResult(Searcher searcher, DocIdAndVersion docIdAndVersion) {
            this(true, docIdAndVersion.version, docIdAndVersion, searcher);
        }

        public boolean exists() {
            return exists;
        }

        public long version() {
            return this.version;
        }

        public Searcher searcher() {
            return this.searcher;
        }

        public DocIdAndVersion docIdAndVersion() {
            return docIdAndVersion;
        }

        @Override
        public void close() {
            release();
        }

        public void release() {
            Releasables.close(searcher);
        }
    }

    /**
     * Method to close the engine while the write lock is held.
     * Must decrement the supplied when closing work is done and resources are
     * freed.
     */
    protected abstract void closeNoLock(String reason, CountDownLatch closedLatch);

    /**
     * Flush the engine (committing segments to disk and truncating the
     * translog) and close it.
     */
    public void flushAndClose() throws IOException { // NOTE: htt, 关闭 searchManager, translog, indexWriter, store
        if (isClosed.get() == false) {
            logger.trace("flushAndClose now acquire writeLock");
            try (ReleasableLock lock = writeLock.acquire()) { // NOTE: htt, writeLock的锁在try之后会自动处理
                logger.trace("flushAndClose now acquired writeLock");
                try {
                    logger.debug("flushing shard on close - this might take some time to sync files to disk");
                    try {
                        flush(); // TODO we might force a flush in the future since we have the write lock already even though recoveries are running.
                    } catch (AlreadyClosedException ex) {
                        logger.debug("engine already closed - skipping flushAndClose");
                    }
                } finally {
                    close(); // double close is not a problem
                }
            }
        }
        awaitPendingClose();
    }

    @Override
    public void close() throws IOException {
        if (isClosed.get() == false) { // don't acquire the write lock if we are already closed
            logger.debug("close now acquiring writeLock");
            try (ReleasableLock lock = writeLock.acquire()) {
                logger.debug("close acquired writeLock");
                closeNoLock("api", closedLatch); // NOTE: htt, 关闭translog、indexWriter等
            }
        }
        awaitPendingClose();
    }

    private void awaitPendingClose() {
        try {
            closedLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static class CommitId implements Writeable { // NOTE: htt, 提交的id

        private final byte[] id;

        public CommitId(byte[] id) {
            assert id != null;
            this.id = Arrays.copyOf(id, id.length);
        }

        /**
         * Read from a stream.
         */
        public CommitId(StreamInput in) throws IOException {
            assert in != null;
            this.id = in.readByteArray();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeByteArray(id);
        }

        @Override
        public String toString() {
            return Base64.getEncoder().encodeToString(id);
        }

        public boolean idsEqual(byte[] id) {
            return Arrays.equals(id, this.id);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            CommitId commitId = (CommitId) o;

            if (!Arrays.equals(id, commitId.id)) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(id);
        }
    }

    public static class IndexCommitRef implements Closeable { // NOTE: htt, 提供close()接口以带 onClose.run()
        private final AtomicBoolean closed = new AtomicBoolean();
        private final CheckedRunnable<IOException> onClose; // NOTE: htt, close操作
        private final IndexCommit indexCommit; // NOTE: htt, commit info of an index

        IndexCommitRef(IndexCommit indexCommit, CheckedRunnable<IOException> onClose) {
            this.indexCommit = indexCommit;
            this.onClose = onClose;
        }

        @Override
        public void close() throws IOException {
            if (closed.compareAndSet(false, true)) {
                onClose.run();
            }
        }

        public IndexCommit getIndexCommit() {
            return indexCommit;
        }
    }

    public void onSettingsChanged() {
    }

    /**
     * Returns the timestamp of the last write in nanoseconds.
     * Note: this time might not be absolutely accurate since the {@link Operation#startTime()} is used which might be
     * slightly inaccurate.
     *
     * @see System#nanoTime()
     * @see Operation#startTime()
     */
    public long getLastWriteNanos() {
        return this.lastWriteNanos;
    }

    /**
     * Called for each new opened engine searcher to warm new segments
     *
     * @see EngineConfig#getWarmer()
     */
    public interface Warmer { // NOTE: htt, get new searcher to warm new segments
        /**
         * Called once a new Searcher is opened on the top-level searcher.
         */
        void warm(Engine.Searcher searcher);
    }

    /**
     * Request that this engine throttle incoming indexing requests to one thread.  Must be matched by a later call to {@link #deactivateThrottling()}.
     */
    public abstract void activateThrottling();

    /**
     * Reverses a previous {@link #activateThrottling} call.
     */
    public abstract void deactivateThrottling();

    /**
     * Marks operations in the translog as completed. This is used to restore the state of the local checkpoint tracker on primary
     * promotion.
     *
     * @throws IOException if an I/O exception occurred reading the translog
     */
    public abstract void restoreLocalCheckpointFromTranslog() throws IOException; // NOTE: htt, 从translog中恢复 local checkPoint

    /**
     * Fills up the local checkpoints history with no-ops until the local checkpoint
     * and the max seen sequence ID are identical.
     * @param primaryTerm the shards primary term this engine was created for
     * @return the number of no-ops added
     */
    public abstract int fillSeqNoGaps(long primaryTerm) throws IOException;

    /**
     * Performs recovery from the transaction log.
     * This operation will close the engine if the recovery fails.
     */
    public abstract Engine recoverFromTranslog() throws IOException; // NOTE: htt, 从translog中恢复数据

    /**
     * Do not replay translog operations, but make the engine be ready.
     */
    public abstract void skipTranslogRecovery();

    /**
     * Returns <code>true</code> iff this engine is currently recovering from translog.
     */
    public boolean isRecovering() { // NOTE: htt, 是否从 translog中恢复
        return false;
    }

    /**
     * Tries to prune buffered deletes from the version map.
     */
    public abstract void maybePruneDeletes();
}
