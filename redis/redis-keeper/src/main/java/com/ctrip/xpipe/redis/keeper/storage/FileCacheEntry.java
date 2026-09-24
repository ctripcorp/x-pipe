package com.ctrip.xpipe.redis.keeper.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;

import com.ctrip.xpipe.tuple.Pair;

class FileCacheEntry {
    private static final Logger logger = LoggerFactory.getLogger(FileCacheEntry.class);

    final CacheMemoryTracker memoryTracker;
    final boolean evictable;
    int refCount = 0;
    boolean writerOpen = false;
    volatile boolean fsInconsistent = false;
    // Offsets strictly before this are not trusted on local disk.
    volatile long localReadableFromOffset = 0;
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
            pendingFsyncBytes = 0;
            writtenToFsOffset = 0;
            fsInconsistent = false;
            localReadableFromOffset = 0;
        }
    }

    void clear() {
        releaseAllChunks();
        cacheStartOffset = 0;
        cacheEndOffset = 0;
        writtenGen = 0;
        cacheGen = 0;
        writtenToFsOffset = 0;
        pendingFsyncBytes = 0;
        localReadableFromOffset = 0;
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

    boolean isCacheDirty(boolean atomicReplace) {
        return atomicReplace ? cacheGen != writtenGen : cacheEndOffset > writtenToFsOffset;
    }

    boolean isFsyncDirty() {
        return pendingFsyncBytes > 0;
    }

    void appendToChunkedCache(ByteBuf data, long nowNanos, long chunkSize) {
        long chunkIdx = cacheEndOffset / chunkSize;
        int inChunk = (int) (cacheEndOffset % chunkSize);
        int totalBytes = data.readableBytes();
        while (data.isReadable()) {
            int remaining = (int) (chunkSize - inChunk);
            int len = data.readableBytes();
            boolean chunkFull = len >= remaining;
            if (chunkFull) len = remaining;
            CacheChunk cacheChunk = chunks.get(chunkIdx);
            ByteBuf chunk = cacheChunk.buffer;
            chunk.setBytes(inChunk, data, len);
            cacheChunk.lastAppendNanos = nowNanos;
            chunkIdx++;
            inChunk = 0;
        }
        cacheEndOffset += totalBytes;
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

    // Must be called under synchronized(this).
    java.util.List<ByteBuf> collectAtomicChunkSlice(long offset, long end, boolean failOnMissingChunk) {
        java.util.List<ByteBuf> slices = new java.util.ArrayList<>(1);
        if (offset >= end) {
            return slices;
        }
        CacheChunk cacheChunk = chunks.get(0L);
        int length = (int) (end - offset);
        if (cacheChunk == null || end > cacheEndOffset) {
            if (failOnMissingChunk) {
                throw new CacheChunksNotContinuousException(
                        "atomic cache chunk 0 with size " + cacheEndOffset
                                + " does not cover range [" + offset + ", " + end + ")");
            }
            return slices;
        }
        slices.add(cacheChunk.buffer.retainedSlice((int) offset, length));
        return slices;
    }

    // Must be called under synchronized(this).
    java.util.List<ByteBuf> collectCacheSlices(long offset, long end, boolean failOnMissingChunk,
            boolean atomicReplace, long chunkSize) {
        if (atomicReplace) {
            return collectAtomicChunkSlice(offset, end, failOnMissingChunk);
        }
        return collectChunkSlices(offset, end, failOnMissingChunk, chunkSize);
    }

    ByteBuf readWithCache(long length, long offset, boolean atomicReplace, long chunkSize) {
        long end = Math.min(offset + length, cacheEndOffset);
        java.util.List<ByteBuf> slices = collectCacheSlices(offset, end, false, atomicReplace, chunkSize);
        CompositeByteBuf composite = StorageAllocator.ALLOC.compositeDirectBuffer();
        for (ByteBuf s : slices) {
            composite.addComponent(true, s);
        }
        return composite;
    }

    // Empty buf with ioGen == 0: nothing to flush.
    // ioGen == 0 with data: no cache write.
    // ioGen > 0 with data: after FS write, update writtenGen to ioGen.
    Pair<Long, ByteBuf> getPendingAtomicWriteBufAfterInFlight() {
        if (!isInitialized()) {
            return Pair.of(0L, Unpooled.buffer(0));
        }
        if (writtenGen > cacheGen) {
            logger.warn("atomic cache generation {} is behind written generation {}, advancing it",
                    cacheGen, writtenGen);
            cacheGen = writtenGen + 1;
        }
        if (cacheGen == writtenGen) {
            return Pair.of(0L, Unpooled.buffer(0));
        }
        if (cacheEndOffset <= cacheStartOffset) {
            logger.error("atomic dirty but empty cache range for gen {} (writtenGen {}), fixing writtenGen",
                    cacheGen, writtenGen);
            writtenGen = cacheGen;
            return Pair.of(0L, Unpooled.buffer(0));
        }
        long ioGen = cacheGen;
        java.util.List<ByteBuf> slices = collectAtomicChunkSlice(cacheStartOffset, cacheEndOffset, true);
        return Pair.of(ioGen, slices.get(0));
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
        return buildWriteBufFromCacheRange(writtenToFsOffset, collectEnd, chunkSize);
    }

    ByteBuf buildWriteBufFromCacheRange(long from, long to, long chunkSize) {
        if (from >= to) {
            return Unpooled.buffer(0);
        }
        java.util.List<ByteBuf> pending = collectChunkSlices(from, to, true, chunkSize);
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
