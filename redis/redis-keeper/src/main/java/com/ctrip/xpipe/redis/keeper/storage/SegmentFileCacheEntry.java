package com.ctrip.xpipe.redis.keeper.storage;

import com.ctrip.xpipe.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;

final class SegmentFileCacheEntry extends FileCacheEntry {
    private static final Logger logger = LoggerFactory.getLogger(SegmentFileCacheEntry.class);

    // Reserve all index files for the segment referenced by chunks.
    final ConcurrentHashMap<Long, ConcurrentHashMap<String, FileCacheEntry>> indexFiles = new ConcurrentHashMap<>();
    // Segment starts that currently hold a writer index cache lease.
    final LinkedList<Long> writerIndexLeaseStarts = new LinkedList<>();

    SegmentFileCacheEntry(CacheMemoryTracker memoryTracker) {
        super(memoryTracker, true);
    }

    Pair<Boolean, FileCacheEntry> acquireIndexFileCacheEntry(long startOffset, String indexPrefix, boolean write) {
        synchronized (this) {
            ConcurrentHashMap<String, FileCacheEntry> byPrefix =
                    indexFiles.computeIfAbsent(startOffset, k -> new ConcurrentHashMap<>());
            FileCacheEntry entry = byPrefix.computeIfAbsent(
                    indexPrefix, k -> new FileCacheEntry(memoryTracker, false));
            return Pair.of(entry.retainEntry(write), entry);
        }
    }

    void bindWriterIndexLease(long startOffset) {
        synchronized (this) {
            if (!writerIndexLeaseStarts.isEmpty()
                    && startOffset <= writerIndexLeaseStarts.getLast()) {
                return;
            }
            ConcurrentHashMap<String, FileCacheEntry> byPrefix = indexFiles.get(startOffset);
            if (byPrefix == null || byPrefix.isEmpty()) {
                return;
            }
            writerIndexLeaseStarts.addLast(startOffset);
            for (FileCacheEntry entry : byPrefix.values()) {
                entry.refCount++;
            }
        }
    }

    // Must be called under synchronized(this).
    // Drop leases for segment starts strictly before newCacheStart.
    private void releaseWriterIndexLeasesThrough(long newCacheStart) {
        while (!writerIndexLeaseStarts.isEmpty()
                && writerIndexLeaseStarts.getFirst() < newCacheStart) {
            releaseWriterIndexLease(writerIndexLeaseStarts.removeFirst());
        }
    }


    @Override
    void reset() {
        synchronized (this) {
            if (cacheStartOffset < 0) return;
            releaseAllWriterIndexLeases();
            for (ConcurrentHashMap<String, FileCacheEntry> byPrefix : indexFiles.values()) {
                for (FileCacheEntry indexEntry : byPrefix.values()) {
                    synchronized (indexEntry) {
                        if (indexEntry.cacheStartOffset >= 0) {
                            indexEntry.reset();
                        }
                    }
                }
            }
            super.reset();
        }
    }

    // Must be called under synchronized(this).
    @Override
    void clear() {
        if (cacheStartOffset < 0) return;
        releaseAllWriterIndexLeases();
        for (ConcurrentHashMap<String, FileCacheEntry> byPrefix : indexFiles.values()) {
            for (FileCacheEntry indexEntry : byPrefix.values()) {
                synchronized (indexEntry) {
                    if (indexEntry.cacheStartOffset >= 0) {
                        indexEntry.clear();
                    }
                }
            }
        }
        super.clear();
    }

    @Override
    void dropCacheBefore(long newStartOffset, long chunkSize) {
        releaseWriterIndexLeasesThrough(newStartOffset);
        super.dropCacheBefore(newStartOffset, chunkSize);
    }

    // Must be called under synchronized(this).
    private void resetSegmentCache(long offset, long newWrittenToFsOffset) {
        releaseAllChunks();
        cacheStartOffset = offset;
        cacheEndOffset = offset;
        writtenToFsOffset = newWrittenToFsOffset;
        releaseAllWriterIndexLeases();
    }

    // Must be called under synchronized(this).
    // Drop leases for segment starts strictly after truncateOffset (segments deleted by truncate).
    private void releaseWriterIndexLeasesAfter(long truncateOffset) {
        while (!writerIndexLeaseStarts.isEmpty()
                && writerIndexLeaseStarts.getLast() > truncateOffset) {
            releaseWriterIndexLease(writerIndexLeaseStarts.removeLast());
        }
    }

    // Must be called under synchronized(this).
    void truncateTo(long offset, long chunkSize, long prevStartOffset) {
        if (offset < prevStartOffset || offset > cacheEndOffset) {
            resetSegmentCache(offset, offset);
        } else if (offset <= cacheStartOffset) {
            resetSegmentCache(offset, Math.min(offset, writtenToFsOffset));
        } else if (offset < cacheEndOffset) {
            super.truncateTo(offset, chunkSize);
            releaseWriterIndexLeasesAfter(offset);
        }
        // offset == cacheEndOffset: nothing to do
    }

    // Must be called under synchronized(this).
    private void releaseAllWriterIndexLeases() {
        while (!writerIndexLeaseStarts.isEmpty()) {
            releaseWriterIndexLease(writerIndexLeaseStarts.removeFirst());
        }
    }

    // Must be called under synchronized(this).
    private void releaseWriterIndexLease(long startOffset) {
        ConcurrentHashMap<String, FileCacheEntry> byPrefix = indexFiles.get(startOffset);
        if (byPrefix == null) {
            // bindWriterIndexLease only records startOffset when index entries exist; null here is unexpected.
            logger.error("writer index lease has no index map at offset {}", startOffset);
            return;
        }
        for (String indexPrefix : byPrefix.keySet()) {
            releaseIndexFileCacheEntry(startOffset, indexPrefix, false);
        }
    }

    void releaseIndexFileCacheEntry(long startOffset, String indexPrefix, boolean write) {
        synchronized (this) {
            ConcurrentHashMap<String, FileCacheEntry> byPrefix = indexFiles.get(startOffset);
            if (byPrefix == null) {
                return;
            }
            releaseIndexFileCacheEntry(startOffset, indexPrefix, write, byPrefix.get(indexPrefix));
        }
    }

    void releaseIndexFileCacheEntry(long startOffset, String indexPrefix, boolean write,
            FileCacheEntry entry) {
        if (entry == null) {
            return;
        }
        synchronized (this) {
            if (!entry.releaseEntry(write)) {
                return;
            }
            ConcurrentHashMap<String, FileCacheEntry> byPrefix = indexFiles.get(startOffset);
            if (byPrefix == null) {
                return;
            }
            byPrefix.remove(indexPrefix, entry);
            if (byPrefix.isEmpty()) {
                indexFiles.remove(startOffset);
            }
        }
    }

    @Override
    long cacheSizeBytes() {
        long bytes = super.cacheSizeBytes();
        for (ConcurrentHashMap<String, FileCacheEntry> byPrefix : indexFiles.values()) {
            for (FileCacheEntry indexEntry : byPrefix.values()) {
                bytes += indexEntry.cacheSizeBytes();
            }
        }
        return bytes;
    }

    @Override
    boolean releaseEntry(boolean write) {
        boolean released = super.releaseEntry(write);
        if (released) {
            for (ConcurrentHashMap<String, FileCacheEntry> byPrefix : indexFiles.values()) {
                for (FileCacheEntry indexEntry : byPrefix.values()) {
                    indexEntry.releaseAllChunks();
                }
            }
            indexFiles.clear();
            writerIndexLeaseStarts.clear();
        }
        return released;
    }
}
