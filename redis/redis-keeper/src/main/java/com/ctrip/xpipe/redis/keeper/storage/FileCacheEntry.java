package com.ctrip.xpipe.redis.keeper.storage;

import io.netty.buffer.ByteBuf;

import java.util.concurrent.ConcurrentHashMap;

class FileCacheEntry {
    int refCount;
    boolean writerOpen = false;
    // -1: no cache data yet
    volatile long cacheStartOffset = -1;
    // Exclusive upper bound of all cached data; also the max readable offset for the file.
    volatile long cacheEndOffset = 0;
    // Exclusive upper bound of data written to the backing FS.
    volatile long writtenToFsOffset = 0;
    final ConcurrentHashMap<Long, ByteBuf> chunks = new ConcurrentHashMap<>();
    // FULL_CACHE only: writer-side initialization may overwrite reader-side preload, but reader-side preload
    // must never overwrite cache contents that were initialized by a writer.
    volatile boolean fullCacheInitializedByWriter = false;
}
