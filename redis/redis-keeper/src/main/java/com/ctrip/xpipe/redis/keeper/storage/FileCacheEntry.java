package com.ctrip.xpipe.redis.keeper.storage;

import io.netty.buffer.ByteBuf;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

class FileCacheEntry {
    int refCount;
    boolean writerOpen = false;
    final CountDownLatch initDone = new CountDownLatch(1);
    // -1: no cache data yet
    volatile long cacheStartOffset = -1;
    // Exclusive upper bound of all cached data; also the max readable offset for the file.
    volatile long cacheEndOffset = 0;
    // Exclusive upper bound of data written to the backing FS.
    volatile long writtenToFsOffset = 0;
    // Atomic FULL_CACHE content version, used to track dirty state.
    long cacheGen = 0;
    long writtenGen = 0;
    final ConcurrentHashMap<Long, ByteBuf> chunks = new ConcurrentHashMap<>();
}
