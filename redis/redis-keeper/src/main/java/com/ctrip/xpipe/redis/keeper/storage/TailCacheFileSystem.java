package com.ctrip.xpipe.redis.keeper.storage;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;

import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.ctrip.xpipe.redis.keeper.storage.AbstractStorageFile.CacheMode;
import com.ctrip.xpipe.redis.keeper.storage.TailCacheFileSystemConfig.BackingFsMode;
import com.ctrip.xpipe.tuple.Pair;

import io.netty.buffer.Unpooled;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * No idle/background flush for residual cache below writeBatchBytes when writes stop:
 * streaming workloads keep writing; non-streaming callers are expected to close() (or fsync())
 * to seal. In current use, small loss in extreme cases is recoverable externally.
 * <p>
 * If idle/async flush is added later, a workable approach is to tighten in-flight ownership:
 * change await into a locked await-and-register (wait prior IO, then claim the slot under the
 * same lock), and change register into bind-future (attach the real IO future to the claimed
 * slot). That removes the await/register TOCTOU that would otherwise race with write/close.
 * A claimed placeholder must count as in-flight; after claim, always bind the real future or
 * release the claim, including on failure paths.
 */
public class TailCacheFileSystem implements AsyncFileSystem {

    private static final Logger logger = LoggerFactory.getLogger(TailCacheFileSystem.class);
    private static final int LOCK_STRIPES = 32;

    private final AsyncFileSystem delegate;
    private volatile boolean readPreferCache;
    private volatile boolean transferPreferCache;
    private volatile BackingFsMode backingFsMode;
    private volatile long maxCacheSizeBytes;
    private volatile long maxCacheSizePerTenantBytes;
    private volatile long expectedMinRetentionMs;
    private final long chunkSize;
    private volatile int preloadChunkThreshold;
    private volatile long preloadTimeoutMs;
    private volatile long ioWaitTimeoutMs;
    private volatile long writeBatchBytes;
    private volatile int maxWriteChunkThreshold;
    private volatile int eioRetryMaxAttempts;
    private final ExecutorService ioExecutor;

    private final ConcurrentHashMap<String, FileCacheEntry> fileCacheEntries = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, SegmentFileCacheEntry> segmentCacheEntries = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CompletableFuture<?>> inFlightIo = new ConcurrentHashMap<>();

    private final Object[] locks = new Object[LOCK_STRIPES];

    public TailCacheFileSystem(AsyncFileSystem delegate, TailCacheFileSystemConfig config, ExecutorService ioExecutor) {
        this.delegate = delegate;
        this.readPreferCache = config.isReadPreferCache();
        this.transferPreferCache = config.isTransferPreferCache();
        this.backingFsMode = config.getBackingFsMode();
        this.maxCacheSizeBytes = config.getMaxCacheSizeBytes();
        this.maxCacheSizePerTenantBytes = config.getMaxCacheSizePerTenantBytes();
        this.expectedMinRetentionMs = config.getExpectedMinRetentionMs();
        this.chunkSize = config.getChunkSize();
        this.preloadChunkThreshold = config.getPreloadChunkThreshold();
        this.preloadTimeoutMs = config.getPreloadTimeoutMs();
        this.ioWaitTimeoutMs = config.getIoWaitTimeoutMs();
        this.writeBatchBytes = config.getWriteBatchBytes();
        this.maxWriteChunkThreshold = config.getMaxWriteChunkThreshold();
        this.eioRetryMaxAttempts = config.getEioRetryMaxAttempts();
        this.ioExecutor = ioExecutor;
        for (int i = 0; i < LOCK_STRIPES; i++) locks[i] = new Object();
    }

    public boolean isReadPreferCache() {
        return readPreferCache;
    }

    public void setReadPreferCache(boolean readPreferCache) {
        this.readPreferCache = readPreferCache;
    }

    public boolean isTransferPreferCache() {
        return transferPreferCache;
    }

    public void setTransferPreferCache(boolean transferPreferCache) {
        this.transferPreferCache = transferPreferCache;
    }

    public BackingFsMode getBackingFsMode() {
        return backingFsMode;
    }

    public void setBackingFsMode(BackingFsMode backingFsMode) {
        this.backingFsMode = backingFsMode;
    }

    public long getMaxCacheSizeBytes() {
        return maxCacheSizeBytes;
    }

    public void setMaxCacheSizeBytes(long maxCacheSizeBytes) {
        this.maxCacheSizeBytes = maxCacheSizeBytes;
    }

    public long getMaxCacheSizePerTenantBytes() {
        return maxCacheSizePerTenantBytes;
    }

    public void setMaxCacheSizePerTenantBytes(long maxCacheSizePerTenantBytes) {
        this.maxCacheSizePerTenantBytes = maxCacheSizePerTenantBytes;
    }

    public long getExpectedMinRetentionMs() {
        return expectedMinRetentionMs;
    }

    public void setExpectedMinRetentionMs(long expectedMinRetentionMs) {
        this.expectedMinRetentionMs = expectedMinRetentionMs;
    }

    private CacheMode resolveFileCacheMode(boolean atomicReplace, CacheMode override) {
        if (override != null) {
            if (atomicReplace && override == CacheMode.TAIL_CACHE) {
                throw new IllegalArgumentException("TAIL_CACHE is not supported for atomicReplace");
            }
            return override;
        }
        return atomicReplace ? CacheMode.FULL_CACHE : CacheMode.TAIL_CACHE;
    }

    private CacheMode resolveSegmentCacheMode(CacheMode override) {
        if (override != null) {
            if (override == CacheMode.FULL_CACHE) {
                throw new IllegalArgumentException("FULL_CACHE is not supported for segment files");
            }
            return override;
        }
        return CacheMode.TAIL_CACHE;
    }

    public int getPreloadChunkThreshold() {
        return preloadChunkThreshold;
    }

    public void setPreloadChunkThreshold(int preloadChunkThreshold) {
        if (preloadChunkThreshold <= 0) throw new IllegalArgumentException("preloadChunkThreshold must be positive");
        this.preloadChunkThreshold = preloadChunkThreshold;
    }

    public long getPreloadTimeoutMs() {
        return preloadTimeoutMs;
    }

    public void setPreloadTimeoutMs(long preloadTimeoutMs) {
        if (preloadTimeoutMs < 0) throw new IllegalArgumentException("preloadTimeoutMs must be non-negative");
        this.preloadTimeoutMs = preloadTimeoutMs;
    }

    public long getIoWaitTimeoutMs() {
        return ioWaitTimeoutMs;
    }

    public void setIoWaitTimeoutMs(long ioWaitTimeoutMs) {
        if (ioWaitTimeoutMs < 0) throw new IllegalArgumentException("ioWaitTimeoutMs must be non-negative");
        this.ioWaitTimeoutMs = ioWaitTimeoutMs;
    }

    public long getWriteBatchBytes() {
        return writeBatchBytes;
    }

    public void setWriteBatchBytes(long writeBatchBytes) {
        if (writeBatchBytes <= 0) throw new IllegalArgumentException("writeBatchBytes must be positive");
        this.writeBatchBytes = writeBatchBytes;
    }

    public int getMaxWriteChunkThreshold() {
        return maxWriteChunkThreshold;
    }

    public void setMaxWriteChunkThreshold(int maxWriteChunkThreshold) {
        if (maxWriteChunkThreshold <= 0) throw new IllegalArgumentException("maxWriteChunkThreshold must be positive");
        this.maxWriteChunkThreshold = maxWriteChunkThreshold;
    }

    public int getEioRetryMaxAttempts() {
        return eioRetryMaxAttempts;
    }

    public void setEioRetryMaxAttempts(int eioRetryMaxAttempts) {
        if (eioRetryMaxAttempts <= 0) throw new IllegalArgumentException("eioRetryMaxAttempts must be positive");
        this.eioRetryMaxAttempts = eioRetryMaxAttempts;
    }

    @Override
    public void shutdown() {
        delegate.shutdown();
    }

    // ---- lock helpers ----

    private Object lockFor(String key) {
        return locks[(key.hashCode() & 0x7fffffff) % LOCK_STRIPES];
    }

    // Must be called under lockFor(key).
    private Pair<Boolean, FileCacheEntry> acquireFileCacheEntry(String key, boolean write) {
        FileCacheEntry entry = fileCacheEntries.computeIfAbsent(key, k -> new FileCacheEntry());
        if (write) {
            if (entry.writerOpen) throw new IllegalStateException("writer already open for " + key);
            entry.writerOpen = true;
        }
        entry.refCount++;
        return Pair.of(entry.refCount == 1, entry);
    }

    // Must be called under lockFor(key).
    private void releaseFileCacheEntry(String key, boolean write) {
        FileCacheEntry entry = fileCacheEntries.get(key);
        if (entry == null) return;
        if (write) entry.writerOpen = false;
        if (--entry.refCount == 0) {
            fileCacheEntries.remove(key);
            synchronized (entry) {
                releaseAllChunks(entry);
            }
        }
    }

    // Must be called under synchronized(entry).
    private void releaseAllChunks(FileCacheEntry entry) {
        entry.chunks.values().forEach(buf -> buf.release());
        entry.chunks.clear();
    }

    private boolean hasInFlightIo(String id) {
        return inFlightIo.containsKey(id);
    }

    private void awaitInFlightIo(String id) {
        CompletableFuture<?> f = inFlightIo.get(id);
        if (f == null) {
            return;
        }
        try {
            if (ioWaitTimeoutMs > 0) {
                f.get(ioWaitTimeoutMs, TimeUnit.MILLISECONDS);
            } else {
                f.get();
            }
        } catch (TimeoutException e) {
            throw new OperationNotExecutedException(id, e);
        } catch (Exception e) {
            logger.warn("prior IO completed or failed for {}, ignoring during wait", id, e);
        }
    }

    private void registerInFlight(String id, CompletableFuture<?> op) {
        inFlightIo.put(id, op);
        op.whenComplete((r, e) -> inFlightIo.remove(id, op));
    }

    // require file inflight io to be completed before calling this
    private void flushPendingWriteAndAwait(AbstractStorageFile file,
            java.util.function.Function<ByteBuf, Long> fsWrite, boolean failIfStillDirty) {
        if (!file.canWrite() || file.getCacheEntry() == null) {
            return;
        }
        FileCacheEntry entry = file.getCacheEntry();
        if (entry.cacheStartOffset < 0) {
            return;
        }
        boolean dirty = file.atomicReplace
                ? entry.cacheGen != entry.writtenGen
                : entry.cacheEndOffset > entry.writtenToFsOffset;
        if (!dirty) {
            return;
        }
        final String id = file.getKey();
        final ByteBuf writeBuf;
        final long ioGen;
        if (file.atomicReplace) {
            Pair<Long, ByteBuf> atomic = getAtomicBufAfterInFlight(entry, Unpooled.buffer(0), true);
            writeBuf = atomic.getValue();
            ioGen = atomic.getKey();
        } else {
            writeBuf = buildWriteBufFromCache(entry, Long.MAX_VALUE);
            ioGen = 0;
        }
        if (!writeBuf.isReadable()) {
            writeBuf.release();
            return;
        }
        CompletableFuture<Long> flushFuture = StorageUtil.supply(ioExecutor, () -> {
            StorageUtil.requireCacheOpen(file);
            long written = fsWrite.apply(writeBuf);
            synchronized (entry) {
                if (file.atomicReplace) {
                    if (ioGen > entry.writtenGen) {
                        entry.writtenGen = ioGen;
                    }
                    entry.writtenToFsOffset = written;
                } else {
                    entry.writtenToFsOffset += written;
                }
                return written;
            }
        });
        registerInFlight(id, flushFuture);
        awaitInFlightIo(id);
        Throwable flushError = null;
        boolean retryableFailure = false;
        try {
            flushFuture.get(1, TimeUnit.NANOSECONDS);
        } catch (ExecutionException e) {
            flushError = e.getCause();
            retryableFailure = flushError instanceof EIOException;
        } catch (TimeoutException e) {
            flushError = e;
            retryableFailure = true;
        } catch (Exception e) {
            flushError = e;
        }

        boolean stillDirty = file.atomicReplace
                ? entry.cacheGen != entry.writtenGen
                : entry.cacheEndOffset > entry.writtenToFsOffset;
        if (stillDirty) {
            if (retryableFailure) {
                throw new OperationNotExecutedException(id, flushError);
            }
            if (failIfStillDirty) {
                throw new IllegalStateException(
                        "unflushed cache remains after drain for " + id + "; refusing write to avoid corruption",
                        flushError);
            }
            long remainingBytes = Math.max(0, entry.cacheEndOffset - entry.writtenToFsOffset);
            if (flushError != null) {
                logger.error("data lost for {}, still has {} bytes not flushed after flush, cacheGen={}, writtenGen={}",
                        id, remainingBytes, entry.cacheGen, entry.writtenGen, flushError);
            } else {
                logger.error("data lost for {}, still has {} bytes not flushed after flush with non error, cacheGen={}, writtenGen={}",
                        id, remainingBytes, entry.cacheGen, entry.writtenGen);
            }
            return;
        }
        if (flushError != null) {
            logger.error("flush finished with error for {}, unexpected", id, flushError);
        }
    }

    // require file inflight io to be completed before calling this
    private void asyncFileFlushPendingWriteAndAwait(AsyncFile file, boolean failIfStillDirty) {
        flushPendingWriteAndAwait(file, writeBuf -> executeWithEioRetry(file, () -> {
            long written = delegate.writeSync(file, writeBuf);
            delegate.fsyncSync(file);
            return written;
        }), failIfStillDirty);
    }

    // require segment inflight io to be completed before calling this
    private void segmentFlushPendingWriteAndAwait(AsyncSegmentFile file, boolean failIfStillDirty) {
        flushPendingWriteAndAwait(file, writeBuf -> executeWithEioRetry(file, () -> {
            long written = delegate.writeSync(file, writeBuf);
            delegate.fsyncSync(file);
            return written;
        }), failIfStillDirty);
        for (AsyncIndexFile indexFile : file.currentIndexFiles.values()) {
            awaitInFlightIo(indexFile.getKey());
            asyncFileFlushPendingWriteAndAwait(indexFile, failIfStillDirty);
        }
    }

    private <F extends AbstractStorageFile, T> T executeWithEioRetry(F file,
            java.util.function.Supplier<Long> getCurrentWrittenToFsOffset,
            java.util.function.Supplier<T> ioAction) {
        int maxAttempts = eioRetryMaxAttempts;
        try {
            return ioAction.get();
        } catch (RuntimeException e) {
            if (!(e instanceof EIOException)) {
                throw e;
            }
            logger.warn("io action got EIO for {}, retrying reopen up to {} times", file.getKey(), maxAttempts, e);
            for (int attempt = 1; attempt <= maxAttempts; attempt++) {
                try {
                    file.reopenCurrentChannel();
                    resetWrittenToFsOffsetIfNeeded(file, getCurrentWrittenToFsOffset);
                    break;
                } catch (Exception retryError) {
                    logger.error("retry attempt {}/{} failed for {}", attempt, maxAttempts, file.getKey(), retryError);
                }
            }
            throw e;
        }
    }

    private <T> T executeWithEioRetry(AsyncFile file, java.util.function.Supplier<T> ioAction) {
        return executeWithEioRetry(file, () -> delegate.sizeSync(file), ioAction);
    }

    private <T> T executeWithEioRetry(AsyncSegmentFile file, java.util.function.Supplier<T> ioAction) {
        return executeWithEioRetry(file, () -> segmentExclusiveEndOffset(file), ioAction);
    }

    private <F extends AbstractStorageFile> void resetWrittenToFsOffsetIfNeeded(F file,
            java.util.function.Supplier<Long> getCurrentWrittenToFsOffset) throws Exception {
        if (!file.canWrite()) {
            return;
        }
        FileCacheEntry entry = file.getCacheEntry();
        if (entry == null) {
            return;
        }
        if (file.currentWriteChannel() == null) {
            return;
        }
        long fileSize = awaitIoCachePrep(file, getCurrentWrittenToFsOffset);
        synchronized (entry) {
            entry.writtenToFsOffset = fileSize;
        }
    }

    // Must be called under lockFor(key).
    private Pair<Boolean, SegmentFileCacheEntry> acquireSegmentCacheEntry(String key, boolean write) {
        SegmentFileCacheEntry entry = segmentCacheEntries.computeIfAbsent(key, k -> new SegmentFileCacheEntry());
        if (write) {
            if (entry.writerOpen) throw new IllegalStateException("writer already open for " + key);
            entry.writerOpen = true;
        }
        entry.refCount++;
        return Pair.of(entry.refCount == 1, entry);
    }

    // Must be called under lockFor(key).
    private void releaseSegmentCacheEntry(String key, boolean write) {
        SegmentFileCacheEntry entry = segmentCacheEntries.get(key);
        if (entry == null) return;
        if (write) entry.writerOpen = false;
        if (--entry.refCount == 0) {
            segmentCacheEntries.remove(key);
            releaseAllChunks(entry);
            entry.indexFiles.values().forEach(indexFileMap ->
                    indexFileMap.values().forEach(indexEntry -> {
                        releaseAllChunks(indexEntry);
                    }));
            entry.indexFiles.clear();
            entry.writerIndexLeaseStarts.clear();
        }
    }

    // ---- AsyncFile ----

    @Override
    public CompletableFuture<AsyncFile> open(String path, AbstractStorageFile.OpenMode openMode, boolean atomicReplace, boolean lenient, String tenant) {
        return StorageUtil.supply(ioExecutor, () -> openFileSync(path, openMode, atomicReplace, lenient, tenant, null));
    }

    public CompletableFuture<AsyncFile> open(String path, AbstractStorageFile.OpenMode openMode, boolean atomicReplace, boolean lenient, String tenant, CacheMode cacheMode) {
        return StorageUtil.supply(ioExecutor, () -> openFileSync(path, openMode, atomicReplace, lenient, tenant, cacheMode));
    }


    private AsyncFile openFileSync(String path, AbstractStorageFile.OpenMode openMode, boolean atomicReplace, boolean lenient, String tenant, CacheMode cacheModeOverride) {
        CacheMode cacheMode = resolveFileCacheMode(atomicReplace, cacheModeOverride);
        AbstractStorageFile.OpenMode effectiveOpenMode = openMode;
        if (openMode == AbstractStorageFile.OpenMode.WRITE && cacheMode == CacheMode.FULL_CACHE) {
            effectiveOpenMode = AbstractStorageFile.OpenMode.READ_WRITE;
        }
        AsyncFile file = delegate.openSync(path, effectiveOpenMode, atomicReplace, lenient, tenant);
        file.cacheMode = cacheMode;
        if (cacheMode != CacheMode.NO_CACHE) {
            String key = file.getKey();
            boolean write = file.canWrite();
            Pair<Boolean, FileCacheEntry> acquired;
            try {
                synchronized (lockFor(key)) {
                    acquired = acquireFileCacheEntry(key, write);
                }
            } catch (Throwable t) {
                logger.error("acquireFileCacheEntry failed for {}, closing file", file.path, t);
                cleanupOpenFailedFile(file);
                throw t;
            }
            file.cacheEntry = acquired.getValue();
            file.onCacheClose = () -> {
                synchronized (lockFor(key)) {
                    releaseFileCacheEntry(key, write);
                }
            };
            initFileCache(file, acquired.getKey());
        }
        return file;
    }

    private void cleanupOpenFailedFile(AsyncFile file) {
        try {
            file.onCacheClose.run();
        } catch (Throwable t) {
            logger.error("failed to release cache entry for {}", file.path, t);
        }
        try {
            delegate.closeSync(file);
        } catch (Throwable t) {
            logger.error("closeSync failed during open cleanup for {}", file.path, t);
        }
    }

    private void cleanupOpenFailedSegment(AsyncSegmentFile file) {
        try {
            file.onCacheClose.run();
        } catch (Throwable t) {
            logger.error("failed to release segment cache entry for {}", file.getKey(), t);
        }
        try {
            delegate.closeSync(file);
        } catch (Throwable t) {
            logger.error("closeSync failed during open cleanup for {}", file.getKey(), t);
        }
    }

    private boolean shouldPreloadCache(AsyncFile file, boolean useCacheSnapshot) {
        if (!useCacheSnapshot || file.cacheMode != CacheMode.FULL_CACHE) return false;
        return file.cacheEntry.cacheStartOffset < 0;
    }

    private boolean shouldInitTailCache(AbstractStorageFile file, boolean useCacheSnapshot) {
        if (!useCacheSnapshot || file.cacheMode != CacheMode.TAIL_CACHE) {
            return false;
        }
        return file.cacheEntry.cacheStartOffset < 0;
    }

    private boolean initCache(FileCacheEntry entry, boolean first, java.util.function.Supplier<Boolean> init) {
        if (first) {
            boolean ok = init.get();
            entry.initDone.countDown();
            return ok;
        }
        try {
            entry.initDone.await();
            return true;
        } catch (Throwable t) {
            logger.warn("await cache init failed", t);
            return false;
        }
    }

    private boolean initFileCache(AsyncFile file, boolean first) {
        return initCache(file.cacheEntry, first, () -> {
            boolean useCacheSnapshot = useCache(file);
            boolean ok = true;
            if (shouldInitTailCache(file, useCacheSnapshot)) {
                ok &= initTailCacheSync(file, () -> delegate.sizeSync(file));
            }
            if (shouldPreloadCache(file, useCacheSnapshot)) {
                ok &= loadFullFileCache(file, useCacheSnapshot);
            }
            return ok;
        });
    }

    private boolean initSegmentCache(AsyncSegmentFile file, boolean first) {
        return initCache(file.cacheEntry, first, () -> {
            boolean useCacheSnapshot = useCache(file);
            if (shouldInitTailCache(file, useCacheSnapshot)) {
                return initTailCacheSync(file, () -> segmentExclusiveEndOffset(file));
            }
            return true;
        });
    }

    private boolean initTailCacheSync(AbstractStorageFile file,
            java.util.function.Supplier<Long> endOffsetSupplier) {
        try {
            long endOffset = awaitIoCachePrep(file,
                    () -> executeWithEioRetry(file, endOffsetSupplier, endOffsetSupplier));
            FileCacheEntry entry = file.cacheEntry;
            synchronized (entry) {
                if (entry.cacheStartOffset >= 0) {
                    return true;
                }
                entry.cacheStartOffset = endOffset;
                entry.cacheEndOffset = endOffset;
                entry.writtenToFsOffset = endOffset;
            }
            return true;
        } catch (Throwable t) {
            logger.warn("initTailCacheSync failed for {}", file.getKey(), t);
            return false;
        }
    }

    private <T> T awaitIoCachePrep(AbstractStorageFile file, java.util.function.Supplier<T> task) throws Exception {
        CompletableFuture<T> future = StorageUtil.supply(ioExecutor, () -> {
            StorageUtil.requireCacheOpen(file);
            return task.get();
        });
        if (preloadTimeoutMs > 0) {
            return future.get(preloadTimeoutMs, TimeUnit.MILLISECONDS);
        } else {
            return future.get();
        }
    }

    private boolean loadFullFileCache(AsyncFile file, boolean useCacheSnapshot) {
        try {
            Pair<Boolean, ByteBuf> fullData = awaitIoCachePrep(file, () -> readFullData(file));
            boolean aligned = fullData.getKey();
            ByteBuf data = fullData.getValue();
            try {
                synchronized (file.cacheEntry) {
                    if (shouldPreloadCache(file, useCacheSnapshot)) {
                        FileCacheEntry entry = file.cacheEntry;
                        releaseAllChunks(entry);
                        long actualSize = data.readableBytes();
                        if (actualSize == 0) {
                            entry.cacheStartOffset = 0;
                            entry.cacheEndOffset = 0;
                            entry.writtenToFsOffset = 0;
                        } else {
                            long offset = 0;
                            long loadEnd = aligned ? data.capacity() : actualSize;
                            while (offset < loadEnd) {
                                long chunkIdx = offset / chunkSize;
                                ByteBuf chunk;
                                if (aligned) {
                                    chunk = data.retainedSlice((int) offset, (int) chunkSize);
                                } else {
                                    chunk = StorageAllocator.ALLOC.directBuffer((int) chunkSize);
                                    chunk.writeBytes(data, (int) offset, (int) Math.min(chunkSize, actualSize - offset));
                                }
                                entry.chunks.put(chunkIdx, chunk);
                                offset += chunkSize;
                            }
                            entry.cacheStartOffset = 0;
                            entry.cacheEndOffset = actualSize;
                            entry.writtenToFsOffset = actualSize;
                        }
                    }
                }
            } finally {
                data.release();
            }
            return true;
        } catch (Throwable t) {
            logger.error("loadFullCacheFromFile failed for {}", file.path, t);
            return false;
        }
    }

    private Pair<Boolean, ByteBuf> readFullData(AsyncFile file) {
        long fileSize = executeWithEioRetry(file,
                () -> delegate.sizeSync(file));
        boolean aligned = true;
        if (fileSize == 0) return new Pair<>(true, Unpooled.buffer(0));
        ByteBuf data;
        if (fileSize <= preloadChunkThreshold * chunkSize) {
            // small file: aligned read — buffer capacity rounded up to chunkSize multiples,
            // so each chunk slice maps directly onto an aligned region without copying.
            data = executeWithEioRetry(file,
                    () -> delegate.readSync(file, fileSize, 0, chunkSize));
        } else {
            // large file: single read, copy into per-chunk buffers
            aligned = false;
            data = executeWithEioRetry(file,
                    () -> delegate.readSync(file, fileSize, 0, 0));
        }
        return new Pair<>(aligned, data);
    }

    @Override
    public CompletableFuture<Boolean> isFile(AsyncFile file) {
        return delegate.isFile(file);
    }

    @Override
    public CompletableFuture<Boolean> isDirectory(String path) {
        return delegate.isDirectory(path);
    }

    @Override
    public CompletableFuture<Long> lastModified(AsyncFile file) {
        return delegate.lastModified(file);
    }

    @Override
    public CompletableFuture<Void> position(AsyncFile file, long position) {
        if (!file.canRead()) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("position() requires read mode"));
        }
        try {
            StorageUtil.requireCacheOpen(file);
            delegate.positionSync(file, position);
            return CompletableFuture.completedFuture(null);
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
    }


    @Override
    public CompletableFuture<ByteBuf> read(AsyncFile file, long length, long offset) {
        return readInternal(file, length, offset, false,
                () -> executeWithEioRetry(file, () -> delegate.readSync(file, length, offset, 0)));
    }

    @Override
    public CompletableFuture<ByteBuf> read(AsyncFile file, long length) {
        long readOffset = file.position;
        return readInternal(file, length, 0, true,
                () -> executeWithEioRetry(file, () -> delegate.readSync(file, length, readOffset, 0)));
    }

    private CompletableFuture<ByteBuf> readInternal(AbstractStorageFile file, long length, long offset,
            boolean fromPosition, java.util.function.Supplier<ByteBuf> fsRead) {
        StorageUtil.requireCacheOpen(file);
        FileCacheEntry entry = file.getCacheEntry();
        long readOffset = fromPosition ? file.position : offset;
        ByteBuf cached = null;
        if (!preferDirectRead(file, entry, readOffset, readPreferCache)) {
            synchronized (entry) {
                if (inCacheRange(entry, readOffset)) {
                    cached = readWithCache(length, readOffset, entry);
                }
            }
        }
        if (cached != null) {
            if (fromPosition) {
                file.position = readOffset + cached.readableBytes();
            }
            return CompletableFuture.completedFuture(cached);
        }

        return StorageUtil.supply(ioExecutor, () -> {
            StorageUtil.requireCacheOpen(file);
            ByteBuf buf = fsRead.get();
            if (fromPosition) {
                file.position = readOffset + buf.readableBytes();
            }
            return buf;
        });
    }


    // Must be called under synchronized(entry).
    private java.util.List<ByteBuf> collectChunkSlices(FileCacheEntry entry, long offset, long end, boolean failOnMissingChunk) {

        long pos = offset;
        java.util.List<ByteBuf> slices = new java.util.ArrayList<>();
        while (pos < end) {
            long chunkIdx = pos / chunkSize;
            int inChunk = (int) (pos % chunkSize);
            ByteBuf chunk = entry.chunks.get(chunkIdx);
            if (chunk == null) {
                if (!failOnMissingChunk) {
                    break;
                }
                for (ByteBuf slice : slices) {
                    slice.release();
                }
                throw new CacheChunksNotContinuousException(
                        "cache chunks not continuous, missing chunk " + chunkIdx + " for range [" + offset + ", " + end + ")");
            }
            int length = (int) Math.min(chunkSize - inChunk, end - pos);
            slices.add(chunk.retainedSlice(inChunk, length));
            pos += length;
        }
        return slices;
    }

    private ByteBuf readWithCache(long length, long offset, FileCacheEntry entry) {
        long end = Math.min(offset + length, entry.cacheEndOffset);
        java.util.List<ByteBuf> slices = collectChunkSlices(entry, offset, end, false);
        CompositeByteBuf composite = StorageAllocator.ALLOC.compositeDirectBuffer();
        for (ByteBuf s : slices) {
            composite.addComponent(true, s);
        }
        return composite;
    }

    boolean preferDirectRead(AbstractStorageFile file, FileCacheEntry entry, long offset, boolean preferCache) {
        if (file.cacheMode == CacheMode.NO_CACHE) return true;
        if (!inCacheRange(entry, offset)) return true;
        if (!preferCache || backingFsMode == BackingFsMode.NO_CACHE) {
            return offset < entry.writtenToFsOffset;
        }
        return false;
    }

    private boolean inCacheRange(FileCacheEntry entry, long offset) {
        long cacheStart = entry.cacheStartOffset;
        return cacheStart >= 0 && offset >= cacheStart;
    }

    long transferToByCache(FileCacheEntry entry, long offset, long count, WritableByteChannel target) throws IOException {
        java.util.List<ByteBuf> slices;
        synchronized (entry) {
            long end = Math.min(offset + count, entry.cacheEndOffset);
            slices = collectChunkSlices(entry, offset, end, false);
        }
        if (slices.isEmpty()) return 0;
        try {
            if (target instanceof GatheringByteChannel) {
                ByteBuffer[] nioBuffers = new ByteBuffer[slices.size()];
                for (int i = 0; i < slices.size(); i++) {
                    nioBuffers[i] = slices.get(i).nioBuffer();
                }
                return ((GatheringByteChannel) target).write(nioBuffers);
            }

            long transferred = 0;
            for (ByteBuf s : slices) {
                int sliceLength = s.readableBytes();
                ByteBuffer nioSlice = s.nioBuffer();
                int n = target.write(nioSlice);
                transferred += n;
                if (n < sliceLength) break;
            }
            return transferred;
        } finally {
            for (ByteBuf s : slices) s.release();
        }
    }

    @Override
    public CompletableFuture<Long> write(AsyncFile file, ByteBuf data) {
        return writeInternal(file, data,
                cacheWrite -> initCacheAndAppend(file, data, cacheWrite),
                writeBuf -> executeWithEioRetry(file, () -> delegate.writeSync(file, writeBuf)));
    }

    private CompletableFuture<Long> writeInternal(AbstractStorageFile file, ByteBuf data,
            java.util.function.Consumer<Boolean> initCacheAndAppend,
            java.util.function.Function<ByteBuf, Long> fsWrite) {
        if (!file.canWrite()) {
            data.release();
            throw new IllegalArgumentException("operation requires write mode: " + file.getKey());
        }
        if (file.cacheClosed) {
            data.release();
            throw new IllegalStateException("file cache is closed: " + file.getKey());
        }

        FileCacheEntry entry = file.getCacheEntry();
        final long writeSize = data.readableBytes();
        final boolean useCacheSnapshot = useCache(file);
        final String id = file.getKey();
        // First cache build (sizeSync/preload/atomic) must see settled FS; skip wait if cache already live.
        if (useCacheSnapshot && entry != null && entry.cacheStartOffset < 0) {
            try {
                if (hasInFlightIo(id)) {
                    awaitInFlightIo(id);
                }
            } catch (Exception e) {
                data.release();
                throw e;
            }
        }

        initCacheAndAppend.accept(useCacheSnapshot);
        try {
            if (hasInFlightIo(id)) {
                if (!useCacheSnapshot) {
                    awaitInFlightIo(id);
                } else {
                    return CompletableFuture.completedFuture(writeSize);
                }
            }
        } catch (Exception e) {
            if (!useCacheSnapshot) {
                data.release();
            }
            throw e;
        }

        if (!useCacheSnapshot && entry != null && entry.cacheStartOffset >= 0) {
            try {
                flushPendingWriteAndAwait(file, fsWrite, true);
            } catch (RuntimeException e) {
                data.release();
                throw e;
            }
            releaseCacheData(entry);
        }

        final ByteBuf writeBuf;
        final long atomicIoGen;
        if (entry == null || entry.cacheStartOffset < 0) {
            writeBuf = data;
            atomicIoGen = 0;
        } else if (file.atomicReplace) {
            Pair<Long, ByteBuf> atomic = getAtomicBufAfterInFlight(entry, data, useCacheSnapshot);
            writeBuf = atomic.getValue();
            atomicIoGen = atomic.getKey();
        } else {
            writeBuf = buildWriteBufAfterInFlight(entry);
            atomicIoGen = 0;
        }
        if (!writeBuf.isReadable()) {
            writeBuf.release();
            return CompletableFuture.completedFuture(writeSize);
        }
        CompletableFuture<Long> ioFuture = StorageUtil.supply(ioExecutor, () -> {
            StorageUtil.requireCacheOpen(file);
            long written = fsWrite.apply(writeBuf);
            if (entry != null && useCacheSnapshot) {
                synchronized (entry) {
                    if (file.atomicReplace) {
                        if (atomicIoGen > entry.writtenGen) {
                            entry.writtenGen = atomicIoGen;
                        }
                        entry.writtenToFsOffset = written;
                    } else {
                        entry.writtenToFsOffset += written;
                    }
                }
            }
            return writeSize;
        });
        registerInFlight(id, ioFuture);
        if (!useCacheSnapshot) {
            return ioFuture;
        }
        return CompletableFuture.completedFuture(writeSize);
    }

    private boolean useCache(AbstractStorageFile file) {
        return backingFsMode != BackingFsMode.NO_CACHE && file.cacheMode != CacheMode.NO_CACHE;
    }

    private void initCacheAndAppend(AsyncFile file, ByteBuf data, boolean cacheWrite) {
        FileCacheEntry entry = file.getCacheEntry();
        if (entry == null || !cacheWrite) {
            return;
        }
        if (file.atomicReplace) {
            synchronized (entry) {
                releaseAllChunks(entry);
                entry.cacheStartOffset = 0;
                entry.writtenToFsOffset = 0;
                entry.cacheGen++;
                long offset = 0;
                while (data.isReadable()) {
                    long chunkIdx = offset / chunkSize;
                    int len = (int) Math.min(chunkSize, data.readableBytes());
                    ByteBuf chunk = StorageAllocator.ALLOC.directBuffer((int) chunkSize);
                    chunk.writeBytes(data, len);
                    entry.chunks.put(chunkIdx, chunk);
                    offset += len;
                }
                entry.cacheEndOffset = offset;
            }
            data.release();
            return;
        }

        if (shouldPreloadCache(file, cacheWrite)) {
            if (!loadFullFileCache(file, cacheWrite)) {
                data.release();
                throw new PreloadFailedException("preload failed for " + file.path);
            }
        }
        if (shouldInitTailCache(file, cacheWrite)) {
            if (!initTailCacheSync(file, () -> delegate.sizeSync(file))) {
                data.release();
                throw new PreloadFailedException("tail cache init failed for " + file.path);
            }
        }
        appendToFileCache(file, entry, data);
        data.release();
    }

    private void initCacheAndAppend(AsyncSegmentFile file, ByteBuf data, boolean cacheWrite) {
        FileCacheEntry entry = file.getCacheEntry();
        if (entry == null || !cacheWrite) {
            return;
        }
        if (shouldInitTailCache(file, cacheWrite)) {
            if (!initTailCacheSync(file, () -> segmentExclusiveEndOffset(file))) {
                data.release();
                throw new PreloadFailedException("tail cache init failed for " + file.getKey());
            }
        }
        appendToFileCache(file, entry, data);
        data.release();
    }

    private void releaseCacheData(FileCacheEntry entry) {
        synchronized (entry) {
            if (entry instanceof SegmentFileCacheEntry) {
                ((SegmentFileCacheEntry) entry).releaseAllWriterIndexLeases();
            }
            releaseAllChunks(entry);
            entry.cacheStartOffset = -1;
            entry.cacheEndOffset = 0;
            entry.writtenGen = 0;
            entry.cacheGen = 0;
        }
    }

    private void appendToFileCache(AbstractStorageFile file, FileCacheEntry entry, ByteBuf data) {
        synchronized (entry) {
            long offset = entry.cacheEndOffset;
            while (data.isReadable()) {
                long chunkIdx = offset / chunkSize;
                int inChunk = (int) (offset % chunkSize);
                int len = (int) Math.min(chunkSize - inChunk, data.readableBytes());
                ByteBuf chunk = entry.chunks.computeIfAbsent(chunkIdx,
                        k -> StorageAllocator.ALLOC.directBuffer((int) chunkSize));
                chunk.writerIndex(inChunk);
                chunk.writeBytes(data, len);
                offset += len;
            }
            entry.cacheEndOffset = offset;
        }
    }

    // Must be called only when there is no in-flight IO for this file.
    private ByteBuf buildWriteBufAfterInFlight(FileCacheEntry entry) {
        long pending = Math.max(0, entry.cacheEndOffset - entry.writtenToFsOffset);
        if (pending < writeBatchBytes) {
            return Unpooled.buffer(0);
        }
        return buildWriteBufFromCache(entry, maxWriteChunkThreshold * chunkSize);
    }

    // Empty buf with ioGen == 0: nothing to flush.
    // ioGen == 0 with data: no cache write.
    // ioGen > 0 with data: after FS write, update writtenGen to ioGen.
    private Pair<Long, ByteBuf> getAtomicBufAfterInFlight(FileCacheEntry entry, ByteBuf data, boolean cacheWrite) {
        if (!cacheWrite || entry.cacheStartOffset < 0) {
            return Pair.of(0L, data);
        }
        if (entry.writtenGen > entry.cacheGen) {
            entry.cacheGen = entry.writtenGen + 1;
        }
        if (entry.cacheGen == entry.writtenGen) {
            return Pair.of(0L, Unpooled.buffer(0));
        }
        if (entry.cacheEndOffset <= entry.cacheStartOffset) {
            logger.error("atomic dirty but empty cache range for gen {} (writtenGen {}), fixing writtenGen",
                    entry.cacheGen, entry.writtenGen);
            entry.writtenGen = entry.cacheGen;
            return Pair.of(0L, Unpooled.buffer(0));
        }
        long ioGen = entry.cacheGen;
        java.util.List<ByteBuf> slices =
                collectChunkSlices(entry, entry.cacheStartOffset, entry.cacheEndOffset, true);
        CompositeByteBuf composed = StorageAllocator.ALLOC.compositeDirectBuffer();
        for (ByteBuf s : slices) {
            composed.addComponent(true, s);
        }
        return Pair.of(ioGen, composed);
    }

    private ByteBuf buildWriteBufFromCache(FileCacheEntry entry, long maxBytes) {
        long pendingBytes = Math.max(0, entry.cacheEndOffset - entry.writtenToFsOffset);
        if (pendingBytes <= 0) {
            return Unpooled.buffer(0);
        }
        boolean overflow = pendingBytes > maxBytes;
        long collectEnd = overflow
                ? entry.writtenToFsOffset + Math.min(pendingBytes, maxBytes)
                : entry.cacheEndOffset;
        java.util.List<ByteBuf> pending = collectChunkSlices(entry, entry.writtenToFsOffset, collectEnd, true);
        CompositeByteBuf composed = StorageAllocator.ALLOC.compositeDirectBuffer();
        for (ByteBuf s : pending) {
            composed.addComponent(true, s);
        }
        return composed;
    }


    @Override
    public CompletableFuture<Void> delete(String path) {
        return delegate.delete(path);
    }


    @Override
    public CompletableFuture<Void> delete(AsyncFile file) {
        StorageUtil.requireWriteMode(file);
        StorageUtil.requireCacheOpen(file);
        final String id = file.getKey();
        awaitInFlightIo(id);
        FileCacheEntry entry = file.getCacheEntry();
        if (entry != null) {
            synchronized (entry) {
                if (entry.cacheStartOffset >= 0) {
                    releaseAllChunks(entry);
                    entry.cacheStartOffset = 0;
                    entry.cacheEndOffset = 0;
                    entry.writtenToFsOffset = 0;
                }
            }
        }
        return StorageUtil.run(ioExecutor, () -> {
            StorageUtil.requireCacheOpen(file);
            delegate.deleteSync(file.path);
        });
    }

    @Override
    public CompletableFuture<Boolean> exists(String path) {
        return delegate.exists(path);
    }

    @Override
    public CompletableFuture<Long> size(AsyncFile file) {
        StorageUtil.requireCacheOpen(file);
        if (file.cacheMode != CacheMode.NO_CACHE) {
            FileCacheEntry entry = file.getCacheEntry();
            synchronized (entry) {
                if (entry.cacheStartOffset >= 0) {
                    return CompletableFuture.completedFuture(
                            Math.max(entry.writtenToFsOffset, entry.cacheEndOffset));
                }
            }
        }
        return StorageUtil.supply(ioExecutor, () -> {
            StorageUtil.requireCacheOpen(file);
            return executeWithEioRetry(file, () -> delegate.sizeSync(file));
        });
    }


    @Override
    public CompletableFuture<Boolean> mkdir(String path, boolean recursive) {
        return delegate.mkdir(path, recursive);
    }

    @Override
    public CompletableFuture<Boolean> rmdir(String path, boolean recursive) {
        return delegate.rmdir(path, recursive);
    }

    @Override
    public CompletableFuture<Void> truncate(AsyncFile file, long size) {
        StorageUtil.requireWriteMode(file);
        StorageUtil.requireCacheOpen(file);
        final String id = file.getKey();
        awaitInFlightIo(id);
        FileCacheEntry entry = file.getCacheEntry();
        if (entry != null) {
            synchronized (entry) {
                if (entry.cacheStartOffset >= 0) {
                    entry.writtenToFsOffset = Math.min(size, entry.writtenToFsOffset);
                    if (size <= entry.cacheStartOffset) {
                        releaseAllChunks(entry);
                        entry.cacheStartOffset = size;
                        entry.cacheEndOffset = size;
                    } else {
                        long newEnd = Math.min(size, entry.cacheEndOffset);
                        if (newEnd < entry.cacheEndOffset) {
                            long firstDropChunk = (newEnd + chunkSize - 1) / chunkSize;
                            long lastChunk = (entry.cacheEndOffset - 1) / chunkSize;
                            for (long i = firstDropChunk; i <= lastChunk; i++) {
                                ByteBuf chunk = entry.chunks.remove(i);
                                if (chunk != null) chunk.release();
                            }
                            entry.cacheEndOffset = newEnd;
                        }
                    }
                }
            }
        }
        CompletableFuture<Void> ioFuture = StorageUtil.run(ioExecutor, () -> {
            StorageUtil.requireCacheOpen(file);
            executeWithEioRetry(file, () -> {
                delegate.truncateSync(file, size);
                return null;
            });
        });
        registerInFlight(id, ioFuture);
        return ioFuture;
    }


    @Override
    public CompletableFuture<Void> close(AsyncFile file) {
        if (!file.canCloseByUser) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("close is not allowed for: " + file.getKey()));
        }
        return closeInternal(file,
                () -> delegate.closeSync(file),
                f -> asyncFileFlushPendingWriteAndAwait(f, false));
    }

    private <T extends AbstractStorageFile> CompletableFuture<Void> closeInternal(T file,
            Runnable fsClose, java.util.function.Consumer<T> flush) {
        if (file.cacheClosed) {
            return CompletableFuture.completedFuture(null);
        }
        final String id = file.getKey();
        awaitInFlightIo(id);
        flush.accept(file);
        file.cacheClosed = true;
        return StorageUtil.run(ioExecutor, () -> {
            try {
                fsClose.run();
            } finally {
                file.onCacheClose.run();
            }
        });
    }


    @Override
    public CompletableFuture<Void> fsync(AsyncFile file) {
        return fsyncInternal(file,
                () -> asyncFileFlushPendingWriteAndAwait(file, false),
                () -> executeWithEioRetry(file, () -> {
                    delegate.fsyncSync(file);
                    return null;
                }));
    }

    private CompletableFuture<Void> fsyncInternal(AbstractStorageFile file, Runnable flushPending, Runnable fsFsync) {
        StorageUtil.requireWriteMode(file);
        StorageUtil.requireCacheOpen(file);
        final String id = file.getKey();
        awaitInFlightIo(id);
        flushPending.run();
        CompletableFuture<Void> ioFuture = StorageUtil.run(ioExecutor, () -> {
            StorageUtil.requireCacheOpen(file);
            fsFsync.run();
        });
        registerInFlight(id, ioFuture);
        return ioFuture;
    }


    @Override
    public CompletableFuture<List<String>> list(String path) {
        return delegate.list(path);
    }

    @Override
    public CompletableFuture<Long> transferTo(AsyncFile file, long position, long count, WritableByteChannel target) {
        StorageUtil.requireCacheOpen(file);
        return transferToInternal(file, position, count, target,
                () -> executeWithEioRetry(file,
                        () -> delegate.transferToSync(file, position, count, target)));
    }

    private CompletableFuture<Long> transferToInternal(AbstractStorageFile file, long offset, long count,
            WritableByteChannel target, java.util.function.Supplier<Long> fsTransfer) {
        StorageUtil.requireCacheOpen(file);
        FileCacheEntry entry = file.getCacheEntry();
        if (preferDirectRead(file, entry, offset, transferPreferCache)) {
            return StorageUtil.supply(ioExecutor, () -> {
                StorageUtil.requireCacheOpen(file);
                return fsTransfer.get();
            });
        }
        try {
            return CompletableFuture.completedFuture(transferToByCache(entry, offset, count, target));
        } catch (IOException e) {
            return CompletableFuture.failedFuture(new SocketErrorException(e));
        }
    }


    // ---- AsyncSegmentFile ----

    @Override
    public CompletableFuture<AsyncSegmentFile> open(String path, String prefix, List<String> indexPrefixes, boolean write, String tenant) {
        return StorageUtil.supply(ioExecutor, () -> openSegmentSync(path, prefix, indexPrefixes, write, tenant, null));
    }

    public CompletableFuture<AsyncSegmentFile> open(String path, String prefix, List<String> indexPrefixes, boolean write, String tenant, CacheMode cacheMode) {
        return StorageUtil.supply(ioExecutor, () -> openSegmentSync(path, prefix, indexPrefixes, write, tenant, cacheMode));
    }


    private AsyncSegmentFile openSegmentSync(String path, String prefix, List<String> indexPrefixes, boolean write, String tenant, CacheMode cacheModeOverride) {
        CacheMode cacheMode = resolveSegmentCacheMode(cacheModeOverride);
        String key = StorageUtil.segmentKey(path, prefix);
        AsyncSegmentFile file = delegate.openSync(path, prefix, indexPrefixes, write, tenant);
        file.cacheMode = cacheMode;
        if (cacheMode != CacheMode.NO_CACHE) {
            file.setIndexCacheInitializer(this::initFileCache);
            Pair<Boolean, SegmentFileCacheEntry> acquired;
            try {
                synchronized (lockFor(key)) {
                    acquired = acquireSegmentCacheEntry(key, write);
                }
            } catch (Throwable t) {
                logger.error("acquireSegmentCacheEntry failed for {}, closing file", file.getKey(), t);
                cleanupOpenFailedSegment(file);
                throw t;
            }
            file.onCacheClose = () -> {
                synchronized (lockFor(key)) {
                    releaseSegmentCacheEntry(key, write);
                }
            };
            file.setCacheEntry(acquired.getValue());
            initSegmentCache(file, acquired.getKey());
        }
        return file;
    }

    // Exclusive logical end offset of the segment file (firstOffset + size); 0 if empty.
    private long segmentExclusiveEndOffset(AsyncSegmentFile file) {
        List<Long> offsets = list(file);
        return offsets.isEmpty() ? 0L : offsets.get(0) + delegate.sizeSync(file);
    }

    @Override
    public CompletableFuture<Void> position(AsyncSegmentFile file, long offset) {
        if (!file.canRead()) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("position() is not supported in write mode"));
        }
        StorageUtil.requireCacheOpen(file);
        return StorageUtil.run(ioExecutor, () -> {
            StorageUtil.requireCacheOpen(file);
            executeWithEioRetry(file, () -> {
                delegate.positionSync(file, offset);
                return null;
            });
        });
    }


    @Override
    public CompletableFuture<ByteBuf> read(AsyncSegmentFile file, long length) {
        long readOffset = file.position;
        return readInternal(file, length, 0, true,
                () -> executeWithEioRetry(file, () -> delegate.readSync(file, length, readOffset)));
    }


    @Override
    public CompletableFuture<ByteBuf> read(AsyncSegmentFile file, long length, long offset) {
        return readInternal(file, length, offset, false,
                () -> executeWithEioRetry(file, () -> delegate.readSync(file, length, offset)));
    }


    @Override
    public CompletableFuture<Long> write(AsyncSegmentFile file, ByteBuf data) {
        return writeInternal(file, data,
                cacheWrite -> initCacheAndAppend(file, data, cacheWrite),
                writeBuf -> executeWithEioRetry(file, () -> delegate.writeSync(file, writeBuf)));
    }


    @Override
    public CompletableFuture<Map<String, AsyncFile>> roll(AsyncSegmentFile file) {
        StorageUtil.requireWriteMode(file);
        StorageUtil.requireCacheOpen(file);
        final String id = file.getKey();
        awaitInFlightIo(id);
        segmentFlushPendingWriteAndAwait(file, false);
        CompletableFuture<Map<String, AsyncFile>> ioFuture = StorageUtil.supply(ioExecutor, () -> {
            StorageUtil.requireCacheOpen(file);
            return executeWithEioRetry(file, () -> delegate.rollSync(file));
        });
        registerInFlight(id, ioFuture);
        return ioFuture;
    }


    @Override
    public List<Long> list(AsyncSegmentFile file) {
        return delegate.list(file);
    }

    @Override
    public long getCurrentSegmentStartOffset(AsyncSegmentFile file) {
        return delegate.getCurrentSegmentStartOffset(file);
    }

    @Override
    public CompletableFuture<Map<String, AsyncFile>> getCurrentIndexFiles(AsyncSegmentFile file, List<String> indexPrefixes) {
        StorageUtil.requireCacheOpen(file);
        return StorageUtil.supply(ioExecutor, () -> {
            StorageUtil.requireCacheOpen(file);
            return executeWithEioRetry(file, () -> delegate.getCurrentIndexFilesSync(file, indexPrefixes));
        });
    }


    @Override
    public CompletableFuture<Map<String, AsyncFile>> getCurrentIndexFiles(AsyncSegmentFile file) {
        return getCurrentIndexFiles(file, file.indexPrefixes);
    }


    @Override
    public CompletableFuture<Long> size(AsyncSegmentFile file) {
        StorageUtil.requireCacheOpen(file);
        if (file.cacheMode != CacheMode.NO_CACHE) {
            SegmentFileCacheEntry entry = file.getCacheEntry();
            List<Long> offsets = list(file);
            if (offsets.isEmpty()) {
                return CompletableFuture.completedFuture(0L);
            }
            long firstOffset = offsets.get(0);
            synchronized (entry) {
                if (entry.cacheStartOffset >= 0) {
                    long end = Math.max(entry.writtenToFsOffset, entry.cacheEndOffset);
                    return CompletableFuture.completedFuture(Math.max(0L, end - firstOffset));
                }
            }
        }
        return StorageUtil.supply(ioExecutor, () -> {
            StorageUtil.requireCacheOpen(file);
            return executeWithEioRetry(file, () -> delegate.sizeSync(file));
        });
    }


    @Override
    public CompletableFuture<Long> sizeOfSegment(AsyncSegmentFile file, long startOffset) {
        StorageUtil.requireCacheOpen(file);
        if (file.cacheMode != CacheMode.NO_CACHE) {
            List<Long> offsets = list(file);
            if (offsets.isEmpty()) {
                return CompletableFuture.completedFuture(0L);
            }
            int idx = Collections.binarySearch(offsets, startOffset);
            if (idx < 0) {
                return CompletableFuture.completedFuture(0L);
            }
            if (idx + 1 < offsets.size()) {
                return CompletableFuture.completedFuture(offsets.get(idx + 1) - startOffset);
            }
            SegmentFileCacheEntry entry = file.getCacheEntry();
            synchronized (entry) {
                if (entry.cacheStartOffset >= 0) {
                    long end = Math.max(entry.writtenToFsOffset, entry.cacheEndOffset);
                    return CompletableFuture.completedFuture(Math.max(0L, end - startOffset));
                }
            }
        }
        return StorageUtil.supply(ioExecutor, () -> {
            StorageUtil.requireCacheOpen(file);
            return executeWithEioRetry(file, () -> delegate.sizeOfSegmentSync(file, startOffset));
        });
    }


    @Override
    public CompletableFuture<Long> lastModified(AsyncSegmentFile file) {
        return delegate.lastModified(file);
    }

    @Override
    public CompletableFuture<Long> lastModifiedOfSegment(AsyncSegmentFile file, long startOffset) {
        return delegate.lastModifiedOfSegment(file, startOffset);
    }

    @Override
    public CompletableFuture<Void> deleteSegments(AsyncSegmentFile file, List<Long> startOffsets) {
        StorageUtil.requireWriteMode(file);
        StorageUtil.requireCacheOpen(file);
        if (startOffsets.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        List<Long> offsets = list(file);
        int drop = startOffsets.size();
        if (drop >= offsets.size()) {
            throw new IllegalArgumentException("deleteSegments cannot delete the last segment");
        }
        for (int i = 0; i < drop; i++) {
            if (!startOffsets.get(i).equals(offsets.get(i))) {
                throw new IllegalArgumentException(
                        "deleteSegments requires deleting segments in order from the first: expected "
                                + offsets.get(i) + ", got " + startOffsets.get(i));
            }
        }
        long newFirstOffset = offsets.get(drop);
        if (file.cacheMode != CacheMode.NO_CACHE) {
            SegmentFileCacheEntry entry = file.getCacheEntry();
            synchronized (entry) {
                if (entry.cacheStartOffset >= 0) {
                    long cacheStart = entry.cacheStartOffset;
                    long cacheEnd = entry.cacheEndOffset;
                    if (cacheEnd <= newFirstOffset) {
                        // deleteSegments keeps the last segment; cacheEnd should still cover newFirstOffset.
                        throw new IllegalStateException(
                                "segment cache unreliable on deleteSegments for " + file.getKey()
                                        + ": cacheEndOffset=" + cacheEnd
                                        + " <= newFirstOffset=" + newFirstOffset
                                        + ", cacheStartOffset=" + cacheStart
                                        + ", writtenToFsOffset=" + entry.writtenToFsOffset);
                    }
                    if (cacheStart < newFirstOffset) {
                        entry.releaseWriterIndexLeasesThrough(newFirstOffset);
                        long firstDrop = cacheStart / chunkSize;
                        long firstKeep = newFirstOffset / chunkSize;
                        for (long i = firstDrop; i < firstKeep; i++) {
                            ByteBuf chunk = entry.chunks.remove(i);
                            if (chunk != null) {
                                chunk.release();
                            }
                        }
                        entry.cacheStartOffset = newFirstOffset;
                        entry.cacheEndOffset = Math.max(cacheEnd, newFirstOffset);
                    }
                }
            }
        }
        final String id = file.getKey();
        awaitInFlightIo(id);
        CompletableFuture<Void> ioFuture = StorageUtil.run(ioExecutor, () -> {
            StorageUtil.requireCacheOpen(file);
            executeWithEioRetry(file, () -> {
                delegate.deleteSegmentsSync(file, startOffsets);
                return null;
            });
        });
        registerInFlight(id, ioFuture);
        return ioFuture;
    }

    @Override
    public CompletableFuture<Void> delete(AsyncSegmentFile file) {
        StorageUtil.requireWriteMode(file);
        StorageUtil.requireCacheOpen(file);
        final String id = file.getKey();
        awaitInFlightIo(id);
        if (file.cacheMode != CacheMode.NO_CACHE) {
            SegmentFileCacheEntry entry = file.getCacheEntry();
            synchronized (entry) {
                entry.releaseAllWriterIndexLeases();
                // Remaining index entries (still held by open channels): clear cached data.
                for (ConcurrentHashMap<String, FileCacheEntry> byPrefix : entry.indexFiles.values()) {
                    for (FileCacheEntry indexEntry : byPrefix.values()) {
                        synchronized (indexEntry) {
                            if (indexEntry.cacheStartOffset >= 0) {
                                releaseAllChunks(indexEntry);
                                indexEntry.cacheStartOffset = 0;
                                indexEntry.cacheEndOffset = 0;
                                indexEntry.writtenToFsOffset = 0;
                            }
                        }
                    }
                }
                if (entry.cacheStartOffset >= 0) {
                    releaseAllChunks(entry);
                    entry.cacheStartOffset = 0;
                    entry.cacheEndOffset = 0;
                    entry.writtenToFsOffset = 0;
                }
            }
        }
        CompletableFuture<Void> ioFuture = delegate.delete(file);
        return ioFuture;
    }

    @Override
    public CompletableFuture<Map<String, AsyncFile>> truncate(AsyncSegmentFile file, long offset) {
        StorageUtil.requireWriteMode(file);
        StorageUtil.requireCacheOpen(file);
        if (file.cacheMode != CacheMode.NO_CACHE) {
            SegmentFileCacheEntry entry = file.getCacheEntry();
            synchronized (entry) {
                if (entry.cacheStartOffset >= 0) {
                    List<Long> offsets = list(file);
                    boolean outsideSegments = offsets.isEmpty()
                            || offset < offsets.get(0)
                            || offset > entry.cacheEndOffset;
                    if (outsideSegments) {
                        // reset path: truncate to empty then set logical start at offset
                        releaseAllChunks(entry);
                        entry.cacheStartOffset = offset;
                        entry.cacheEndOffset = offset;
                        entry.writtenToFsOffset = offset;
                        entry.releaseAllWriterIndexLeases();
                    } else {
                        entry.writtenToFsOffset = Math.min(offset, entry.writtenToFsOffset);
                        if (offset <= entry.cacheStartOffset) {
                            // Truncate at/before cached range: body cache entirely invalid, but
                            // the containing segment (start <= offset) is kept — only drop later leases.
                            releaseAllChunks(entry);
                            entry.cacheStartOffset = offset;
                            entry.cacheEndOffset = offset;
                            entry.releaseWriterIndexLeasesAfter(offset);
                        } else if (offset < entry.cacheEndOffset) {
                            // Inside cache: drop [offset, cacheEnd).
                            long firstDropChunk = (offset + chunkSize - 1) / chunkSize;
                            long lastChunk = (entry.cacheEndOffset - 1) / chunkSize;
                            for (long i = firstDropChunk; i <= lastChunk; i++) {
                                ByteBuf chunk = entry.chunks.remove(i);
                                if (chunk != null) {
                                    chunk.release();
                                }
                            }
                            entry.cacheEndOffset = offset;
                            entry.releaseWriterIndexLeasesAfter(offset);
                        }
                        // else offset == cacheEndOffset: nothing to drop from cache
                    }
                }
            }
        }
        final String id = file.getKey();
        awaitInFlightIo(id);
        CompletableFuture<Map<String, AsyncFile>> ioFuture = StorageUtil.supply(ioExecutor, () -> {
            StorageUtil.requireCacheOpen(file);
            return executeWithEioRetry(file, () -> delegate.truncateSync(file, offset));
        });
        registerInFlight(id, ioFuture);
        return ioFuture;
    }


    @Override
    public CompletableFuture<Void> close(AsyncSegmentFile file) {
        return closeInternal(file, () -> delegate.closeSync(file), f -> segmentFlushPendingWriteAndAwait(f, false));
    }

    @Override
    public CompletableFuture<Void> fsync(AsyncSegmentFile file) {
        return fsyncInternal(file,
                () -> flushPendingWriteAndAwait(file, writeBuf -> executeWithEioRetry(file,
                        () -> delegate.writeSync(file, writeBuf)), false),
                () -> executeWithEioRetry(file, () -> {
                    delegate.fsyncSync(file);
                    return null;
                }));
    }

    @Override
    public CompletableFuture<Long> transferTo(AsyncSegmentFile file, long offset, long count, WritableByteChannel target) {
        return transferToInternal(file, offset, count, target,
                () -> executeWithEioRetry(file,
                        () -> delegate.transferToSync(file, offset, count, target)));
    }

}
