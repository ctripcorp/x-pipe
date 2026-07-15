package com.ctrip.xpipe.redis.keeper.storage;

import java.util.concurrent.ConcurrentHashMap;

final class SegmentFileCacheEntry extends CacheEntry {
    // Reserve all index files for the segment referenced by chunks.
    final ConcurrentHashMap<Long, ConcurrentHashMap<String, FileCacheEntry>> indexFiles = new ConcurrentHashMap<>();
}
