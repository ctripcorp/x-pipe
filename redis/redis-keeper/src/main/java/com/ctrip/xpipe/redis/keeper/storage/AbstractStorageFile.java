package com.ctrip.xpipe.redis.keeper.storage;

import java.nio.channels.FileChannel;

abstract class AbstractStorageFile {

    public enum CacheMode {
        // Resolved at realtime. Cannot be used as defaultCacheMode itself.
        DYNAMIC,
        NO_CACHE,
        // When used as defaultCacheMode, atomicReplace open calls are automatically upgraded to FULL_CACHE.
        // Explicitly passing TAIL_CACHE to an atomicReplace open is an error.
        TAIL_CACHE,
        // Memory is held until close() is called.
        FULL_CACHE
    }

    long pendingFsyncBytes = 0;
    volatile CacheMode cacheMode = CacheMode.NO_CACHE;
    // when true, tail cache will be upgraded to full cache.
    volatile boolean fullCacheOnly = false;
    volatile Runnable onClose = () -> {};

    FileCacheEntry cacheEntry = null;

    FileCacheEntry getCacheEntry() {
        return cacheEntry;
    }

    abstract FileChannel currentWriteChannel();

    abstract String identifier();
}
