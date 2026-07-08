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

import com.ctrip.xpipe.redis.keeper.storage.AbstractStorageFile.CacheMode;
import com.ctrip.xpipe.redis.keeper.storage.TailCacheFileSystemConfig.BackingFsMode;

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
    private volatile CacheMode defaultCacheMode;
    private final int chunkSize;
    private volatile int preloadChunkThreshold;
    private final ExecutorService ioExecutor;

    private final ConcurrentHashMap<String, FileCacheEntry> fileCacheEntries = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, SegmentFileCacheEntry> segmentCacheEntries = new ConcurrentHashMap<>();

    private final Object[] locks = new Object[LOCK_STRIPES];

    public TailCacheFileSystem(AsyncFileSystem delegate, TailCacheFileSystemConfig config, ExecutorService ioExecutor) {
        this.delegate = delegate;
        this.readPreferCache = config.isReadPreferCache();
        this.transferPreferCache = config.isTransferPreferCache();
        this.backingFsMode = config.getBackingFsMode();
        this.maxCacheSizeBytes = config.getMaxCacheSizeBytes();
        this.maxCacheSizePerTenantBytes = config.getMaxCacheSizePerTenantBytes();
        this.expectedMinRetentionMs = config.getExpectedMinRetentionMs();
        this.defaultCacheMode = config.getDefaultCacheMode();
        this.chunkSize = config.getChunkSize();
        this.preloadChunkThreshold = config.getPreloadChunkThreshold();
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

    public CacheMode getDefaultCacheMode() {
        return defaultCacheMode;
    }

    public void setDefaultCacheMode(CacheMode defaultCacheMode) {
        if (defaultCacheMode == CacheMode.DYNAMIC) {
            throw new IllegalArgumentException("DYNAMIC cannot be used as defaultCacheMode");
        }
        this.defaultCacheMode = defaultCacheMode;
    }

    public int getPreloadChunkThreshold() {
        return preloadChunkThreshold;
    }

    public void setPreloadChunkThreshold(int preloadChunkThreshold) {
        if (preloadChunkThreshold <= 0) throw new IllegalArgumentException("preloadChunkThreshold must be positive");
        this.preloadChunkThreshold = preloadChunkThreshold;
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
    private FileCacheEntry acquireFileCacheEntry(String key) {
        FileCacheEntry entry = fileCacheEntries.computeIfAbsent(key, k -> new FileCacheEntry());
        entry.refCount++;
        return entry;
    }

    // Must be called under lockFor(key).
    private void releaseFileCacheEntry(String key) {
        FileCacheEntry entry = fileCacheEntries.get(key);
        if (entry == null) return;
        if (--entry.refCount == 0) {
            fileCacheEntries.remove(key);
            entry.chunks.values().forEach(ByteBuf::release);
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
    public CompletableFuture<AsyncFile> open(String path, boolean write, boolean atomicReplace, boolean lenient, String tenant) {
        return open(path, write, atomicReplace, lenient, tenant, CacheMode.DYNAMIC);
    }

    public CompletableFuture<AsyncFile> open(String path, boolean write, boolean atomicReplace, boolean lenient, String tenant, CacheMode cacheMode) {
        return StorageUtil.supply(ioExecutor, () -> openFileSync(path, write, atomicReplace, lenient, tenant, cacheMode));
    }

    @Override
    public AsyncFile openSync(String path, boolean write, boolean atomicReplace, boolean lenient, String tenant) {
        throw new UnsupportedOperationException();
    }

    private AsyncFile openFileSync(String path, boolean write, boolean atomicReplace, boolean lenient, String tenant, CacheMode cacheMode) {
        AsyncFile file = delegate.openSync(path, write, atomicReplace, lenient, tenant);
        file.cacheMode = cacheMode;
        file.fullCacheOnly = atomicReplace;
        if (cacheMode != CacheMode.NO_CACHE) {
            String key = StorageUtil.fileKey(file.path);
            synchronized (lockFor(key)) {
                file.cacheEntry = acquireFileCacheEntry(key);
            }
            file.onClose = () -> {
                synchronized (lockFor(key)) {
                    releaseFileCacheEntry(key);
                }
            };
            if (shouldPreloadCache(cacheMode, atomicReplace)) {
                try {
                    preloadCache(file);
                } catch (Throwable t) {
                    logger.error("preloadCache failed for {}, closing file", file.path, t);
                    try {
                        delegate.closeSync(file);
                    } catch (Throwable ce) {
                        logger.error("closeSync failed after preloadCache failure for {}", file.path, ce);
                    }
                    throw t;
                }
            }
        }
        return file;
    }

    private boolean shouldPreloadCache(CacheMode cacheMode, boolean atomicReplace) {
        if (cacheMode == CacheMode.FULL_CACHE) return true;
        if (cacheMode == CacheMode.DYNAMIC) {
            if (defaultCacheMode == CacheMode.FULL_CACHE) return true;
            if (defaultCacheMode == CacheMode.TAIL_CACHE && atomicReplace) return true;
        }
        return false;
    }

    private void preloadCache(AsyncFile file) {
        FileCacheEntry entry = file.cacheEntry;
        long fileSize = delegate.sizeSync(file);
        if (fileSize == 0) return;

        if (fileSize <= (long) preloadChunkThreshold * chunkSize) {
            // small file: aligned read — buffer capacity rounded up to chunkSize multiples,
            // so each chunk slice maps directly onto an aligned region without copying.
            ByteBuf data = delegate.readSync(file, fileSize, 0, chunkSize);
            try {
                long actualSize = data.readableBytes();
                long offset = 0;
                while (offset < data.capacity()) {
                    long chunkIdx = offset / chunkSize;
                    entry.chunks.put(chunkIdx, data.retainedSlice((int) offset, chunkSize));
                    offset += chunkSize;
                }
                entry.cacheStartOffset = 0;
                entry.cacheEndOffset = actualSize;
                entry.writtenToFsOffset = actualSize;
            } finally {
                data.release();
            }
        } else {
            // large file: single read, copy into per-chunk buffers
            ByteBuf data = delegate.readSync(file, fileSize, 0, 0);
            try {
                long offset = 0;
                long actualSize = data.readableBytes();
                while (offset < actualSize) {
                    long chunkIdx = offset / chunkSize;
                    int dataLen = (int) Math.min(chunkSize, actualSize - offset);
                    ByteBuf chunk = StorageAllocator.ALLOC.directBuffer(chunkSize);
                    chunk.writeBytes(data, (int) offset, dataLen);
                    entry.chunks.put(chunkIdx, chunk);
                    offset += dataLen;
                }
                entry.cacheStartOffset = 0;
                entry.cacheEndOffset = actualSize;
                entry.writtenToFsOffset = actualSize;
            } finally {
                data.release();
            }
        }
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
        file.position = position;
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<ByteBuf> read(AsyncFile file, long length, long offset) {
        FileCacheEntry entry = file.getCacheEntry();
        if (preferDirectRead(file.cacheMode, entry, offset, readPreferCache)) {
            return delegate.read(file, length, offset);
        }
        return CompletableFuture.completedFuture(readWithCache(length, offset, entry));
    }

    @Override
    public CompletableFuture<ByteBuf> read(AsyncFile file, long length) {
        FileCacheEntry entry = file.getCacheEntry();
        long pos = file.position;
        if (preferDirectRead(file.cacheMode, entry, pos, readPreferCache)) {
            return StorageUtil.supply(ioExecutor, () -> {
                ByteBuf buf = delegate.readSync(file, length, pos, 0);
                file.position += buf.readableBytes();
                return buf;
            });
        }
        ByteBuf buf = readWithCache(length, pos, entry);
        file.position += buf.readableBytes();
        return CompletableFuture.completedFuture(buf);
    }

    @Override
    public ByteBuf readSync(AsyncFile file, long length, long offset, int alignSize) {
        throw new UnsupportedOperationException();
    }

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

    boolean preferDirectRead(CacheMode mode, FileCacheEntry entry, long offset, boolean preferCache) {
        if (entry == null || mode == CacheMode.NO_CACHE) return true;
        long cacheStart = entry.cacheStartOffset;
        if (cacheStart < 0 || offset < cacheStart) return true;
        if ((mode == CacheMode.DYNAMIC && defaultCacheMode == CacheMode.NO_CACHE) || !preferCache) {
            return offset < entry.writtenToFsOffset;
        }
        return false;
    }

    long transferToByCache(FileCacheEntry entry, long offset, long count, WritableByteChannel target) throws IOException {
        long end = Math.min(offset + count, entry.cacheEndOffset);
        java.util.List<ByteBuf> slices = collectChunkSlices(entry, offset, end);
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
        return delegate.write(file, data);
    }

    @Override
    public CompletableFuture<Void> delete(String path) {
        return delegate.delete(path);
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
        return delegate.sizeSync(file);
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
        return delegate.truncate(file, size);
    }

    @Override
    public CompletableFuture<Void> close(AsyncFile file) {
        return delegate.close(file);
    }

    @Override
    public void closeSync(AsyncFile file) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> fsync(AsyncFile file) {
        return delegate.fsync(file);
    }

    @Override
    public CompletableFuture<List<String>> list(String path) {
        return delegate.list(path);
    }

    @Override
    public CompletableFuture<Long> transferTo(AsyncFile file, long position, long count, WritableByteChannel target) {
        FileCacheEntry entry = file.getCacheEntry();
        if (preferDirectRead(file.cacheMode, entry, position, transferPreferCache)) {
            return delegate.transferTo(file, position, count, target);
        }
        try {
            return CompletableFuture.completedFuture(transferToByCache(entry, position, count, target));
        } catch (IOException e) {
            return CompletableFuture.failedFuture(new SocketErrorException(e));
        }
    }

    // ---- AsyncSegmentFile ----

    @Override
    public CompletableFuture<AsyncSegmentFile> open(String path, String prefix, List<String> indexPrefixes, boolean write, String tenant) {
        return open(path, prefix, indexPrefixes, write, tenant, CacheMode.DYNAMIC);
    }

    public CompletableFuture<AsyncSegmentFile> open(String path, String prefix, List<String> indexPrefixes, boolean write, String tenant, CacheMode cacheMode) {
        return StorageUtil.supply(ioExecutor, () -> openSegmentSync(path, prefix, indexPrefixes, write, tenant, cacheMode));
    }

    @Override
    public AsyncSegmentFile openSync(String path, String prefix, List<String> indexPrefixes, boolean write, String tenant) {
        throw new UnsupportedOperationException();
    }

    private AsyncSegmentFile openSegmentSync(String path, String prefix, List<String> indexPrefixes, boolean write, String tenant, CacheMode cacheMode) {
        String key = StorageUtil.segmentKey(path, prefix);
        AsyncSegmentFile file = delegate.openSync(path, prefix, indexPrefixes, write, tenant);
        file.cacheMode = cacheMode;
        if (cacheMode != CacheMode.NO_CACHE) {
            synchronized (lockFor(key)) {
                acquireSegmentCacheEntry(key);
            }
            file.onClose = () -> {
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
        return delegate.write(file, data);
    }

    @Override
    public CompletableFuture<Map<String, AsyncFile>> roll(AsyncSegmentFile file) {
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
    public CompletableFuture<Map<String, AsyncFile>> openIndexFiles(AsyncSegmentFile file, long startOffset) {
        return delegate.openIndexFiles(file, startOffset);
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
        return delegate.deleteSegments(file, startOffsets);
    }

    @Override
    public CompletableFuture<Void> delete(AsyncSegmentFile file) {
        return delegate.delete(file);
    }

    @Override
    public CompletableFuture<Map<String, AsyncFile>> truncate(AsyncSegmentFile file, long offset) {
        return delegate.truncate(file, offset);
    }

    @Override
    public CompletableFuture<Void> close(AsyncSegmentFile file) {
        return delegate.close(file);
    }

    @Override
    public void closeSync(AsyncSegmentFile file) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> fsync(AsyncSegmentFile file) {
        return delegate.fsync(file);
    }

    @Override
    public CompletableFuture<Long> transferTo(AsyncSegmentFile file, long offset, long count, WritableByteChannel target) {
        return delegate.transferTo(file, offset, count, target);
    }
}
