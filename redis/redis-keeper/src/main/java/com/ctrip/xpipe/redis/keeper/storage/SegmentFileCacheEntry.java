package com.ctrip.xpipe.redis.keeper.storage;

import java.util.concurrent.ConcurrentHashMap;

final class SegmentFileCacheEntry extends FileCacheEntry {
    // Reserve all index files for the segment referenced by chunks.
    final ConcurrentHashMap<Long, ConcurrentHashMap<String, FileCacheEntry>> indexFiles = new ConcurrentHashMap<>();
    // Last segment startOffset for which writer index cache leases were taken (first body write).
    private long indexCacheLeaseStart = -1L;

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
    
    void ensureWriterIndexCacheLease(long startOffset) {
        if (indexCacheLeaseStart == startOffset) {
            return;
        }
        synchronized (this) {
            ConcurrentHashMap<String, FileCacheEntry> byPrefix = indexFiles.get(startOffset);
            if (byPrefix != null) {
                for (FileCacheEntry indexEntry : byPrefix.values()) {
                    indexEntry.refCount++;
                }
            }
            indexCacheLeaseStart = startOffset;
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
