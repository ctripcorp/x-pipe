package com.ctrip.xpipe.redis.keeper.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

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
        releaseMemory();
        return true;
    }

    void putChunk(long index, CacheChunk chunk) {
        if (replaceChunk(index, chunk)) {
            logger.error("chunk already exists at {}, replaced unexpectedly", index);
        }
    }

    int removeChunk(long index) {
        CacheChunk removed = chunks.remove(index);
        int capacity = removed.capacity();
        bodySizeBytes -= capacity;
        removed.release();
        return capacity;
    }

    // Returns true if an existing chunk was replaced.
    boolean replaceChunk(long index, CacheChunk chunk) {
        CacheChunk old = chunks.put(index, chunk);
        if (old == null) {
            bodySizeBytes += chunk.capacity();
            return false;
        }
        bodySizeBytes = bodySizeBytes - old.capacity() + chunk.capacity();
        old.release();
        return true;
    }

    void releaseAllChunks() {
        synchronized (this) {
            memoryTracker.release(bodySizeBytes);
            for (CacheChunk chunk : chunks.values()) {
                chunk.release();
            }
            chunks.clear();
            bodySizeBytes = 0;
        }
    }

    void releaseMemory() {
        releaseAllChunks();
    }

    void reset() {
        synchronized (this) {
            releaseMemory();
            cacheStartOffset = -1;
            cacheEndOffset = 0;
            writtenGen = 0;
            cacheGen = 0;
        }
    }

    void clear() {
        releaseMemory();
        cacheStartOffset = 0;
        cacheEndOffset = 0;
        writtenToFsOffset = 0;
    }

    void setAtomicChunk(CacheChunk chunk, long newWrittenToFsOffset) {
        CacheChunk old = chunks.get(0L);
        long oldBytes = old == null ? 0 : old.capacity();
        long delta = chunk.capacity() - oldBytes;
        replaceChunk(0L, chunk);
        if (delta < 0) memoryTracker.release(-delta);
        cacheStartOffset = 0;
        cacheEndOffset = chunk.capacity();
        writtenToFsOffset = newWrittenToFsOffset;
        cacheGen++;
    }

    void truncateTo(long size, long chunkSize) {
        if (size <= cacheStartOffset) {
            releaseMemory();
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
            CacheChunk tailChunk = chunks.get((size - 1) / chunkSize);
            if (tailChunk != null) tailChunk.reopen();
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

    long bodySizeBytes() {
        return bodySizeBytes;
    }

    long cacheSizeBytes() {
        return bodySizeBytes;
    }
}
