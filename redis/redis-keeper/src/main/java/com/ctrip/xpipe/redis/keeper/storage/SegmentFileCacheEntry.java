package com.ctrip.xpipe.redis.keeper.storage;

import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;

final class SegmentFileCacheEntry extends FileCacheEntry {
    // Reserve all index files for the segment referenced by chunks.
    final ConcurrentHashMap<Long, ConcurrentHashMap<String, FileCacheEntry>> indexFiles = new ConcurrentHashMap<>();
    // Segment starts that currently hold a writer index cache lease.
    final LinkedList<Long> writerIndexLeaseStarts = new LinkedList<>();

    FileCacheEntry acquireIndexFileCacheEntry(long startOffset, String indexPrefix, boolean write) {
        synchronized (this) {
            ConcurrentHashMap<String, FileCacheEntry> byPrefix =
                    indexFiles.computeIfAbsent(startOffset, k -> new ConcurrentHashMap<>());
            FileCacheEntry entry = byPrefix.computeIfAbsent(indexPrefix, k -> new FileCacheEntry());
            if (write) {
                if (entry.writerOpen) {
                    throw new IllegalStateException(
                            "writer already open for index " + indexPrefix + " at offset " + startOffset);
                }
                entry.writerOpen = true;
            }
            entry.refCount++;
            return entry;
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
    void releaseWriterIndexLeasesThrough(long newCacheStart) {
        while (!writerIndexLeaseStarts.isEmpty()
                && writerIndexLeaseStarts.getFirst() < newCacheStart) {
            releaseWriterIndexLease(writerIndexLeaseStarts.removeFirst());
        }
    }

    // Must be called under synchronized(this).
    // Drop leases for segment starts strictly after truncateOffset (segments deleted by truncate).
    void releaseWriterIndexLeasesAfter(long truncateOffset) {
        while (!writerIndexLeaseStarts.isEmpty()
                && writerIndexLeaseStarts.getLast() > truncateOffset) {
            releaseWriterIndexLease(writerIndexLeaseStarts.removeLast());
        }
    }

    // Must be called under synchronized(this).
    void releaseAllWriterIndexLeases() {
        while (!writerIndexLeaseStarts.isEmpty()) {
            releaseWriterIndexLease(writerIndexLeaseStarts.removeFirst());
        }
    }

    // Must be called under synchronized(this).
    private void releaseWriterIndexLease(long startOffset) {
        ConcurrentHashMap<String, FileCacheEntry> byPrefix = indexFiles.get(startOffset);
        if (byPrefix == null) {
            return;
        }
        for (String indexPrefix : byPrefix.keySet()) {
            releaseIndexFileCacheEntry(startOffset, indexPrefix, false);
        }
    }

    void releaseIndexFileCacheEntry(long startOffset, String indexPrefix, boolean write) {
        FileCacheEntry entry;
        synchronized (this) {
            ConcurrentHashMap<String, FileCacheEntry> byPrefix = indexFiles.get(startOffset);
            if (byPrefix == null) {
                return;
            }
            entry = byPrefix.get(indexPrefix);
            if (entry == null) {
                return;
            }
            if (write) {
                entry.writerOpen = false;
            }
            if (--entry.refCount != 0) {
                return;
            }
            byPrefix.remove(indexPrefix);
            if (byPrefix.isEmpty()) {
                indexFiles.remove(startOffset, byPrefix);
            }
        }
        synchronized (entry) {
            entry.chunks.values().forEach(buf -> buf.release());
            entry.chunks.clear();
        }
    }
}
