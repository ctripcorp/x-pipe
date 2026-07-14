package com.ctrip.xpipe.redis.keeper.storage;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;

import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import com.ctrip.xpipe.redis.keeper.storage.AbstractStorageFile.CacheMode;
import com.ctrip.xpipe.redis.keeper.storage.TailCacheFileSystemConfig.BackingFsMode;
import com.ctrip.xpipe.tuple.Pair;

import io.netty.buffer.Unpooled;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Sync methods (openSync, readSync) are not supported by default; add on demand.
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
        if (override != null) return override;
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
    private FileCacheEntry acquireFileCacheEntry(String key, boolean write) {
        FileCacheEntry entry = fileCacheEntries.computeIfAbsent(key, k -> new FileCacheEntry());
        if (write) {
            if (entry.writerOpen) throw new IllegalStateException("writer already open for " + key);
            entry.writerOpen = true;
        }
        entry.refCount++;
        return entry;
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
        } catch (Exception e) {
            throw new WaitingLastOpException(id, e);
        }
    }

    private void registerInFlight(String id, CompletableFuture<?> op) {
        inFlightIo.put(id, op);
        op.whenComplete((r, e) -> inFlightIo.remove(id, op));
    }

    @SuppressWarnings("unused")
    private void executeWithEioRetry(AbstractStorageFile file, Runnable ioAction, Runnable postEioAction) {
        int maxAttempts = eioRetryMaxAttempts;
        try {
            ioAction.run();
            return;
        } catch (RuntimeException e) {
            if (!(e instanceof EIOException)) {
                throw e;
            }
            logger.warn("io action got EIO for {}, retrying reopen up to {} times", file.identifier(), maxAttempts, e);
            for (int attempt = 1; attempt <= maxAttempts; attempt++) {
                try {
                    file.reopenCurrentChannel();
                    resetWrittenToFsOffsetIfNeeded(file);
                    postEioAction.run();
                    return;
                } catch (Exception reopenError) {
                    logger.error("reopen attempt {}/{} failed for {}", attempt, maxAttempts, file.identifier(), reopenError);
                }
            }
            throw e;
        }
    }

    private void resetWrittenToFsOffsetIfNeeded(AbstractStorageFile file) throws Exception {
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
        long fileSize = awaitIoCachePrep(file, () -> delegate.currentSizeSync(file));
        synchronized (entry) {
            entry.writtenToFsOffset = fileSize;
        }
    }

    // Must be called under lockFor(key).
    private void acquireSegmentCacheEntry(String key) {
        SegmentFileCacheEntry entry = segmentCacheEntries.computeIfAbsent(key, k -> new SegmentFileCacheEntry());
        entry.refCount++;
    }

    // Must be called under lockFor(key).
    private void releaseSegmentCacheEntry(String key) {
        SegmentFileCacheEntry entry = segmentCacheEntries.get(key);
        if (entry == null) return;
        if (--entry.refCount == 0) segmentCacheEntries.remove(key);
    }

    // ---- AsyncFile ----

    @Override
    public CompletableFuture<AsyncFile> open(String path, AbstractStorageFile.OpenMode openMode, boolean atomicReplace, boolean lenient, String tenant) {
        return StorageUtil.supply(ioExecutor, () -> openFileSync(path, openMode, atomicReplace, lenient, tenant, null));
    }

    public CompletableFuture<AsyncFile> open(String path, AbstractStorageFile.OpenMode openMode, boolean atomicReplace, boolean lenient, String tenant, CacheMode cacheMode) {
        return StorageUtil.supply(ioExecutor, () -> openFileSync(path, openMode, atomicReplace, lenient, tenant, cacheMode));
    }

    @Override
    public AsyncFile openSync(String path, AbstractStorageFile.OpenMode openMode, boolean atomicReplace, boolean lenient, String tenant) {
        throw new UnsupportedOperationException();
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
            String key = StorageUtil.fileKey(file.path);
            try {
                synchronized (lockFor(key)) {
                    file.cacheEntry = acquireFileCacheEntry(key, file.canWrite());
                }
            } catch (Throwable t) {
                logger.error("acquireFileCacheEntry failed for {}, closing file", file.path, t);
                cleanupOpenFailedFile(file);
                throw t;
            }
            file.onCacheClose = () -> {
                synchronized (lockFor(key)) {
                    releaseFileCacheEntry(key, file.canWrite());
                }
            };
            if (shouldInitTailCache(file)) {
                initTailCacheSync(file);
            }
            if (shouldPreloadCache(file)) {
                loadFullFileCache(file);
            }
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

    private boolean shouldPreloadCache(AsyncFile file) {
        if (backingFsMode == BackingFsMode.NO_CACHE || file.cacheMode != CacheMode.FULL_CACHE) return false;
        FileCacheEntry entry = file.cacheEntry;
        if (file.canWrite()) return !entry.fullCacheInitializedByWriter;
        return entry.cacheStartOffset < 0;
    }

    private boolean shouldInitTailCache(AsyncFile file) {
        if (!file.canWrite() || file.cacheMode != CacheMode.TAIL_CACHE || backingFsMode == BackingFsMode.NO_CACHE) {
            return false;
        }
        return file.cacheEntry.cacheStartOffset < 0;
    }

    private boolean initTailCacheSync(AsyncFile file) {
        try {
            long fileSize = awaitIoCachePrep(file, () -> delegate.sizeSync(file));
            FileCacheEntry entry = file.cacheEntry;
            synchronized (entry) {
                if (entry.cacheStartOffset >= 0) {
                    return true;
                }
                entry.cacheStartOffset = fileSize;
                entry.cacheEndOffset = fileSize;
                entry.writtenToFsOffset = fileSize;
            }
            return true;
        } catch (Throwable t) {
            logger.warn("initTailCacheSync failed for {}", file.path, t);
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

    private boolean loadFullFileCache(AsyncFile file) {
        try {
            Pair<Boolean, ByteBuf> fullData = awaitIoCachePrep(file, () -> readFullData(file));
            boolean aligned = fullData.getKey();
            ByteBuf data = fullData.getValue();
            try {
                synchronized (file.cacheEntry) {
                    if (shouldPreloadCache(file)) {
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
                        if (file.canWrite()) {
                            file.cacheEntry.fullCacheInitializedByWriter = true;
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
        long fileSize = delegate.sizeSync(file);
        boolean aligned = true;
        if (fileSize == 0) return new Pair<>(true, Unpooled.buffer(0));
        ByteBuf data;
        if (fileSize <= preloadChunkThreshold * chunkSize) {
            // small file: aligned read — buffer capacity rounded up to chunkSize multiples,
            // so each chunk slice maps directly onto an aligned region without copying.
            data = delegate.readSync(file, fileSize, 0, chunkSize);
        } else {
            // large file: single read, copy into per-chunk buffers
            aligned = false;
            data = delegate.readSync(file, fileSize, 0, 0);
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
        return delegate.position(file, position);
    }

    @Override
    public CompletableFuture<ByteBuf> read(AsyncFile file, long length, long offset) {
        return readInternal(file, length, offset, false);
    }

    @Override
    public CompletableFuture<ByteBuf> read(AsyncFile file, long length) {
        return readInternal(file, length, 0, true);
    }

    private CompletableFuture<ByteBuf> readInternal(AsyncFile file, long length, long offset, boolean fromPosition) {
        FileCacheEntry entry = file.getCacheEntry();
        long readOffset = fromPosition ? file.position : offset;
        if (shouldPreloadCache(file)) {
            return StorageUtil.supply(ioExecutor, () -> {
                StorageUtil.requireCacheOpen(file);
                if (!loadFullFileCache(file)) {
                    return Unpooled.buffer(0);
                }
                ByteBuf cached;
                synchronized (entry) {
                    cached = readWithCache(length, readOffset, entry);
                }
                if (fromPosition) {
                    file.position = readOffset + cached.readableBytes();
                }
                return cached;
            });
        }
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
            ByteBuf buf = delegate.readSync(file, length, readOffset, 0);
            if (fromPosition) {
                file.position = readOffset + buf.readableBytes();
            }
            return buf;
        });
    }

    @Override
    public ByteBuf readSync(AsyncFile file, long length, long offset, long alignSize) {
        throw new UnsupportedOperationException();
    }

    // Must be called under synchronized(entry).
    private java.util.List<ByteBuf> collectChunkSlices(FileCacheEntry entry, long offset, long end) {

        long pos = offset;
        java.util.List<ByteBuf> slices = new java.util.ArrayList<>();
        while (pos < end) {
            long chunkIdx = pos / chunkSize;
            int inChunk = (int) (pos % chunkSize);
            ByteBuf chunk = entry.chunks.get(chunkIdx);
            if (chunk == null) break;
            int length = (int) Math.min(chunkSize - inChunk, end - pos);
            slices.add(chunk.retainedSlice(inChunk, length));
            pos += length;
        }
        return slices;
    }

    private ByteBuf readWithCache(long length, long offset, FileCacheEntry entry) {
        long end = Math.min(offset + length, entry.cacheEndOffset);
        java.util.List<ByteBuf> slices = collectChunkSlices(entry, offset, end);
        CompositeByteBuf composite = StorageAllocator.ALLOC.compositeDirectBuffer();
        for (ByteBuf s : slices) {
            composite.addComponent(true, s);
        }
        return composite;
    }

    boolean preferDirectRead(AsyncFile file, FileCacheEntry entry, long offset, boolean preferCache) {
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
            slices = collectChunkSlices(entry, offset, end);
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
        if (!file.canWrite()) {
            data.release();
            throw new IllegalArgumentException("operation requires write mode: " + file.identifier());
        }
        if (file.cacheClosed) {
            data.release();
            throw new IllegalStateException("file cache is closed: " + file.identifier());
        }
        FileCacheEntry entry = file.getCacheEntry();
        final long writeSize = data.readableBytes();
        final ByteBuf writeBuf = initCacheAndBuildWriteBuf(file, data);
        final String id = file.identifier();
        try {
            if (hasInFlightIo(id)) {
                if (!useCache(file)) {
                    awaitInFlightIo(id);
                } else {
                    writeBuf.release();
                    return CompletableFuture.completedFuture(writeSize);
                }
            }
        } catch (WaitingLastOpException e) {
            writeBuf.release();
            return CompletableFuture.failedFuture(e);
        }
        CompletableFuture<Long> ioFuture = StorageUtil.supply(ioExecutor, () -> {
            StorageUtil.requireCacheOpen(file);
            long written = delegate.writeSync(file, writeBuf);
            if (entry != null) {
                synchronized (entry) {
                    entry.writtenToFsOffset += written;
                }
            }
            return writeSize;
        });
        registerInFlight(id, ioFuture);
        if (!useCache(file)) {
            return ioFuture;
        }
        return CompletableFuture.completedFuture(writeSize);
    }

    private boolean useCache(AbstractStorageFile file) {
        return backingFsMode != BackingFsMode.NO_CACHE && file.cacheMode != CacheMode.NO_CACHE;
    }

    private ByteBuf initCacheAndBuildWriteBuf(AsyncFile file, ByteBuf data) {
        FileCacheEntry entry = file.getCacheEntry();
        if (entry == null) {
            return data;
        }
        if (file.atomicReplace) {
            if (useCache(file)) {
                synchronized (entry) {
                    releaseAllChunks(entry);
                    entry.cacheStartOffset = 0;
                    entry.writtenToFsOffset = 0;
                    long offset = 0;
                    int savedReaderIndex = data.readerIndex();
                    while (data.isReadable()) {
                        long chunkIdx = offset / chunkSize;
                        int len = (int) Math.min(chunkSize, data.readableBytes());
                        ByteBuf chunk = StorageAllocator.ALLOC.directBuffer((int) chunkSize);
                        chunk.writeBytes(data, len);
                        entry.chunks.put(chunkIdx, chunk);
                        offset += len;
                    }
                    entry.cacheEndOffset = offset;
                    if (file.cacheMode == CacheMode.FULL_CACHE) {
                        entry.fullCacheInitializedByWriter = true;
                    }
                    data.readerIndex(savedReaderIndex);
                }
            }
            return data;
        }

        boolean cacheWrite = useCache(file);
        if (cacheWrite) {
            if (shouldPreloadCache(file)) {
                if (!loadFullFileCache(file)) {
                    data.release();
                    throw new PreloadFailedException("preload failed for " + file.path);
                }
            }
            if (shouldInitTailCache(file)) {
                if (!initTailCacheSync(file)) {
                    data.release();
                    throw new PreloadFailedException("tail cache init failed for " + file.path);
                }
            }
            synchronized (entry) {
                long offset = entry.cacheEndOffset;
                int savedReaderIndex = data.readerIndex();
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
                if (file.cacheMode == CacheMode.FULL_CACHE) {
                    entry.fullCacheInitializedByWriter = true;
                }
                data.readerIndex(savedReaderIndex);
            }
        }
        if (entry.writtenToFsOffset < entry.cacheEndOffset) {
            synchronized (entry) {
                long maxBytes = cacheWrite ? Long.MAX_VALUE : maxWriteChunkThreshold * chunkSize;
                return buildWriteBufFromCache(entry, data, maxBytes);
            }
        }
        return data;
    }

    // Must be called under synchronized(entry).
    private ByteBuf buildWriteBufFromCache(FileCacheEntry entry, ByteBuf data, long maxBytes) {
        long pendingBytes = entry.cacheEndOffset - entry.writtenToFsOffset;
        if (pendingBytes == 0) {
            return data;
        }
        boolean overflow = pendingBytes + data.readableBytes() > maxBytes;
        long collectEnd = overflow
                ? entry.writtenToFsOffset + Math.min(pendingBytes, maxBytes)
                : entry.cacheEndOffset;
        java.util.List<ByteBuf> pending = collectChunkSlices(entry, entry.writtenToFsOffset, collectEnd);
        CompositeByteBuf composed = StorageAllocator.ALLOC.compositeDirectBuffer();
        for (ByteBuf s : pending) composed.addComponent(true, s);
        if (!overflow) composed.addComponent(true, data);
        else data.release();
        return composed;
    }

    @Override
    public long writeSync(AsyncFile file, ByteBuf data) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> delete(String path) {
        return delegate.delete(path);
    }

    @Override
    public void deleteSync(String path) {
        throw new UnsupportedOperationException();
    }

    public CompletableFuture<Void> delete(AsyncFile file) {
        StorageUtil.requireWriteMode(file);
        return StorageUtil.supply(ioExecutor, () -> {
            StorageUtil.requireCacheOpen(file);
            delegate.deleteSync(file.path);
            FileCacheEntry entry = file.getCacheEntry();
            if (entry != null) {
                synchronized (entry) {
                    releaseAllChunks(entry);
                    entry.cacheStartOffset = -1;
                    entry.cacheEndOffset = 0;
                    entry.writtenToFsOffset = 0;
                    entry.fullCacheInitializedByWriter = false;
                }
            }
            return null;
        });
    }

    @Override
    public CompletableFuture<Boolean> exists(String path) {
        return delegate.exists(path);
    }

    @Override
    public CompletableFuture<Long> size(AsyncFile file) {
        return delegate.size(file);
    }

    @Override
    public long sizeSync(AsyncFile file) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long currentSizeSync(AbstractStorageFile file) {
        throw new UnsupportedOperationException();
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
        final String id = file.identifier();
        try {
            awaitInFlightIo(id);
        } catch (WaitingLastOpException e) {
            return CompletableFuture.failedFuture(e);
        }
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
            delegate.truncateSync(file, size);
        });
        registerInFlight(id, ioFuture);
        if (!useCache(file)) {
            return ioFuture;
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void truncateSync(AsyncFile file, long size) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> close(AsyncFile file) {
        if (file.cacheClosed) {
            return CompletableFuture.completedFuture(null);
        }
        final String id = file.identifier();
        try {
            awaitInFlightIo(id);
        } catch (WaitingLastOpException e) {
            return CompletableFuture.failedFuture(e);
        }
        file.cacheClosed = true;
        return StorageUtil.run(ioExecutor, () -> {
            try {
                delegate.closeSync(file);
            } finally {
                file.onCacheClose.run();
            }
        });
    }

    @Override
    public void closeSync(AsyncFile file) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> fsync(AsyncFile file) {
        StorageUtil.requireWriteMode(file);
        return StorageUtil.run(ioExecutor, () -> {
            StorageUtil.requireCacheOpen(file);
            delegate.fsyncSync(file);
        });
    }

    @Override
    public void fsyncSync(AsyncFile file) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<List<String>> list(String path) {
        return delegate.list(path);
    }

    @Override
    public CompletableFuture<Long> transferTo(AsyncFile file, long position, long count, WritableByteChannel target) {
        FileCacheEntry entry = file.getCacheEntry();
        if (shouldPreloadCache(file)) {
            loadFullFileCache(file);
        }
        if (preferDirectRead(file, entry, position, transferPreferCache)) {
            return delegate.transferTo(file, position, count, target);
        }
        StorageUtil.requireCacheOpen(file);
        try {
            return CompletableFuture.completedFuture(transferToByCache(entry, position, count, target));
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

    @Override
    public AsyncSegmentFile openSync(String path, String prefix, List<String> indexPrefixes, boolean write, String tenant) {
        throw new UnsupportedOperationException();
    }

    private AsyncSegmentFile openSegmentSync(String path, String prefix, List<String> indexPrefixes, boolean write, String tenant, CacheMode cacheModeOverride) {
        CacheMode cacheMode = resolveSegmentCacheMode(cacheModeOverride);
        String key = StorageUtil.segmentKey(path, prefix);
        AsyncSegmentFile file = delegate.openSync(path, prefix, indexPrefixes, write, tenant);
        file.cacheMode = cacheMode;
        if (cacheMode != CacheMode.NO_CACHE) {
            synchronized (lockFor(key)) {
                acquireSegmentCacheEntry(key);
            }
            file.onCacheClose = () -> {
                synchronized (lockFor(key)) {
                    releaseSegmentCacheEntry(key);
                }
            };
        }
        return file;
    }

    @Override
    public CompletableFuture<Void> position(AsyncSegmentFile file, long offset) {
        return delegate.position(file, offset);
    }

    @Override
    public CompletableFuture<ByteBuf> read(AsyncSegmentFile file, long length) {
        return delegate.read(file, length);
    }

    @Override
    public CompletableFuture<ByteBuf> read(AsyncSegmentFile file, long length, long offset) {
        return delegate.read(file, length, offset);
    }

    @Override
    public CompletableFuture<Long> write(AsyncSegmentFile file, ByteBuf data) {
        if (!file.canWrite()) {
            data.release();
            throw new IllegalArgumentException("operation requires write mode: " + file.identifier());
        }
        return delegate.write(file, data);
    }

    @Override
    public CompletableFuture<Map<String, AsyncFile>> roll(AsyncSegmentFile file) {
        StorageUtil.requireWriteMode(file);
        return delegate.roll(file);
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
        return delegate.getCurrentIndexFiles(file, indexPrefixes);
    }

    @Override
    public CompletableFuture<Map<String, AsyncFile>> getCurrentIndexFiles(AsyncSegmentFile file) {
        return delegate.getCurrentIndexFiles(file);
    }

    @Override
    public CompletableFuture<Long> size(AsyncSegmentFile file) {
        return delegate.size(file);
    }

    @Override
    public CompletableFuture<Long> sizeOfSegment(AsyncSegmentFile file, long startOffset) {
        return delegate.sizeOfSegment(file, startOffset);
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
        return delegate.deleteSegments(file, startOffsets);
    }

    @Override
    public CompletableFuture<Void> delete(AsyncSegmentFile file) {
        StorageUtil.requireWriteMode(file);
        return delegate.delete(file);
    }

    // TODO if truncate offset > writtenToFsOffset but < cacheEndOffset, we need to truncate the cache only.
    @Override
    public CompletableFuture<Map<String, AsyncFile>> truncate(AsyncSegmentFile file, long offset) {
        StorageUtil.requireWriteMode(file);
        return delegate.truncate(file, offset);
    }

    @Override
    public CompletableFuture<Void> close(AsyncSegmentFile file) {
        if (file.cacheClosed) {
            return CompletableFuture.completedFuture(null);
        }
        file.cacheClosed = true;
        file.onCacheClose.run();
        CompletableFuture<Void> ioFuture = delegate.close(file);
        if (!useCache(file)) {
            return ioFuture;
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void closeSync(AsyncSegmentFile file) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> fsync(AsyncSegmentFile file) {
        StorageUtil.requireWriteMode(file);
        return delegate.fsync(file);
    }

    @Override
    public CompletableFuture<Long> transferTo(AsyncSegmentFile file, long offset, long count, WritableByteChannel target) {
        return delegate.transferTo(file, offset, count, target);
    }
}
