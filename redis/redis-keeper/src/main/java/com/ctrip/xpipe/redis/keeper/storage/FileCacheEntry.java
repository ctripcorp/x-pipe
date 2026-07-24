package com.ctrip.xpipe.redis.keeper.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;

class FileCacheEntry {
    private static final Logger logger = LoggerFactory.getLogger(FileCacheEntry.class);

    final CacheMemoryTracker memoryTracker;
    final boolean evictable;
    int refCount = 0;
    boolean writerOpen = false;
    volatile boolean largeFile = false;
    final CountDownLatch initDone = new CountDownLatch(1);
    // -1: no cache data yet
    volatile long cacheStartOffset = -1;
    // Exclusive upper bound of all cached data; also the max readable offset for the file.
    volatile long cacheEndOffset = 0;
    // Exclusive upper bound of data written to the backing FS.
    volatile long writtenToFsOffset = 0;
    // Bytes written to FS but not yet fsynced.
    volatile long pendingFsyncBytes = 0;
    // Atomic FULL_CACHE content version, used to track dirty state.
    volatile long cacheGen = 0;
    volatile long writtenGen = 0;
    final ConcurrentHashMap<Long, CacheChunk> chunks = new ConcurrentHashMap<>();
    volatile long bodySizeBytes = 0;

    FileCacheEntry(CacheMemoryTracker memoryTracker, boolean evictable) {
        this.memoryTracker = memoryTracker;
        this.evictable = evictable;
    }

    boolean retainEntry(boolean write) {
        if (write) {
            if (writerOpen) {
                throw new IllegalStateException("writer already open");
            }
            writerOpen = true;
        }
        return ++refCount == 1;
    }

    boolean releaseEntry(boolean write) {
        if (write) {
            writerOpen = false;
        }
        if (--refCount != 0) {
            return false;
        }
        releaseAllChunks();
        return true;
    }

    void putChunk(long index, CacheChunk chunk) {
        if (replaceChunk(index, chunk)) {
            logger.error("chunk already exists at {}, replaced unexpectedly", index);
        }
    }

    private int removeChunk(long index) {
        CacheChunk removed = chunks.remove(index);
        int capacity = removed.buffer.capacity();
        bodySizeBytes -= capacity;
        removed.buffer.release();
        return capacity;
    }

    // Returns true if an existing chunk was replaced.
    private boolean replaceChunk(long index, CacheChunk chunk) {
        CacheChunk old = chunks.put(index, chunk);
        if (old == null) {
            bodySizeBytes += chunk.buffer.capacity();
            return false;
        }
        bodySizeBytes = bodySizeBytes - old.buffer.capacity() + chunk.buffer.capacity();
        old.buffer.release();
        return true;
    }

    protected void releaseAllChunks() {
        synchronized (this) {
            memoryTracker.release(bodySizeBytes);
            for (CacheChunk chunk : chunks.values()) {
                chunk.buffer.release();
            }
            chunks.clear();
            bodySizeBytes = 0;
        }
    }

    void reset() {
        synchronized (this) {
            releaseAllChunks();
            cacheStartOffset = -1;
            cacheEndOffset = 0;
            writtenGen = 0;
            cacheGen = 0;
        }
    }

    void clear() {
        releaseAllChunks();
        cacheStartOffset = 0;
        cacheEndOffset = 0;
        writtenToFsOffset = 0;
    }

    void setAtomicChunk(CacheChunk chunk, long newWrittenToFsOffset) {
        CacheChunk old = chunks.get(0L);
        long oldBytes = old == null ? 0 : old.buffer.capacity();
        long delta = chunk.buffer.capacity() - oldBytes;
        replaceChunk(0L, chunk);
        if (delta < 0) memoryTracker.release(-delta);
        cacheStartOffset = 0;
        cacheEndOffset = chunk.buffer.capacity();
        writtenToFsOffset = newWrittenToFsOffset;
        cacheGen++;
    }

    void truncateTo(long size, long chunkSize) {
        if (size <= cacheStartOffset) {
            releaseAllChunks();
            cacheStartOffset = size;
            cacheEndOffset = size;
        } else {
            long firstDropChunk = (size + chunkSize - 1) / chunkSize;
            long lastChunk = (cacheEndOffset - 1) / chunkSize;
            long dropCount = lastChunk - firstDropChunk + 1;
            for (long i = firstDropChunk; i <= lastChunk; i++) {
                removeChunk(i);
            }
            memoryTracker.release(dropCount * chunkSize);
            cacheEndOffset = size;
        }
        writtenToFsOffset = Math.min(size, writtenToFsOffset);
    }

    void dropCacheBefore(long newStartOffset, long chunkSize) {
        long firstDrop = cacheStartOffset / chunkSize;
        long firstKeep = newStartOffset / chunkSize;
        long dropCount = firstKeep - firstDrop;
        for (long i = firstDrop; i < firstKeep; i++) {
            removeChunk(i);
        }
        memoryTracker.release(dropCount * chunkSize);
        cacheStartOffset = newStartOffset;
    }

    boolean isInitialized() {
        return cacheStartOffset >= 0;
    }

    void appendToChunkedCache(ByteBuf data, long nowNanos, long chunkSize) {
        long offset = cacheEndOffset;
        while (data.isReadable()) {
            long chunkIdx = offset / chunkSize;
            int remaining = (int) (chunkSize - offset % chunkSize);
            int len = data.readableBytes();
            boolean chunkFull = len >= remaining;
            if (chunkFull) len = remaining;
            CacheChunk cacheChunk = chunks.get(chunkIdx);
            ByteBuf chunk = cacheChunk.buffer;
            chunk.writeBytes(data, len);
            cacheChunk.lastAppendNanos = nowNanos;
            offset += len;
        }
        cacheEndOffset = offset;
    }

    private java.util.List<ByteBuf> collectChunkSlices(long offset, long end, boolean failOnMissingChunk, long chunkSize) {
        long pos = offset;
        java.util.List<ByteBuf> slices = new java.util.ArrayList<>();
        while (pos < end) {
            long chunkIdx = pos / chunkSize;
            int inChunk = (int) (pos % chunkSize);
            CacheChunk cacheChunk = chunks.get(chunkIdx);
            if (cacheChunk == null) {
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
            slices.add(cacheChunk.buffer.retainedSlice(inChunk, length));
            pos += length;
        }
        return slices;
    }

    ByteBuf buildWriteBufFromCache(long maxBytes, long chunkSize) {
        long pendingBytes = Math.max(0, cacheEndOffset - writtenToFsOffset);
        if (pendingBytes <= 0) {
            return Unpooled.buffer(0);
        }
        boolean overflow = pendingBytes > maxBytes;
        long collectEnd = overflow
                ? writtenToFsOffset + Math.min(pendingBytes, maxBytes)
                : cacheEndOffset;
        java.util.List<ByteBuf> pending = collectChunkSlices(writtenToFsOffset, collectEnd, true, chunkSize);
        CompositeByteBuf composed = StorageAllocator.ALLOC.compositeDirectBuffer();
        for (ByteBuf s : pending) {
            composed.addComponent(true, s);
        }
        return composed;
    }

    long cacheSizeBytes() {
        return bodySizeBytes;
    }
}
