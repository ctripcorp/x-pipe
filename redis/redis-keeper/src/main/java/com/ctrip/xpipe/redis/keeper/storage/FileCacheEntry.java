package com.ctrip.xpipe.redis.keeper.storage;

import io.netty.buffer.ByteBuf;
import java.util.concurrent.ConcurrentHashMap;

final class FileCacheEntry extends CacheEntry {

    final ConcurrentHashMap<Long, ByteBuf> chunks = new ConcurrentHashMap<>();

    // -1: no cache data yet
    volatile long cacheStartOffset = -1;
    // Exclusive upper bound of all cached data; also the max readable offset for the file.
    volatile long cacheEndOffset = 0;

    // Exclusive upper bound of data written to the backing FS.
    volatile long writtenToFsOffset = 0;
    // Exclusive upper bound of data fsync'd to the backing FS.
    volatile long fsyncedToFsOffset = 0;

    FileCacheEntry() {
    }
}
