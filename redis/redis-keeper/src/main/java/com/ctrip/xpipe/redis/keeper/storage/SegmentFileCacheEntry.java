package com.ctrip.xpipe.redis.keeper.storage;

import java.util.concurrent.ConcurrentHashMap;

final class SegmentFileCacheEntry extends FileCacheEntry {
    // Reserve all index files for the segment referenced by chunks.
    final ConcurrentHashMap<Long, ConcurrentHashMap<String, FileCacheEntry>> indexFiles = new ConcurrentHashMap<>();

    synchronized FileCacheEntry acquireIndexFileCacheEntry(long startOffset, String indexPrefix, boolean write) {
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

    synchronized void releaseIndexFileCacheEntry(long startOffset, String indexPrefix, boolean write) {
        ConcurrentHashMap<String, FileCacheEntry> byPrefix = indexFiles.get(startOffset);
        if (byPrefix == null) {
            return;
        }
        FileCacheEntry entry = byPrefix.get(indexPrefix);
        if (entry == null) {
            return;
        }
        if (write) {
            entry.writerOpen = false;
        }
        if (--entry.refCount == 0) {
            byPrefix.remove(indexPrefix);
            synchronized (entry) {
                entry.chunks.values().forEach(buf -> buf.release());
                entry.chunks.clear();
            }
            if (byPrefix.isEmpty()) {
                indexFiles.remove(startOffset, byPrefix);
            }
        }
    }
}
