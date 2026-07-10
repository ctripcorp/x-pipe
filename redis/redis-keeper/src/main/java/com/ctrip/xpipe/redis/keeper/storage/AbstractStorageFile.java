package com.ctrip.xpipe.redis.keeper.storage;

import java.nio.channels.FileChannel;

abstract class AbstractStorageFile {

    public enum CacheMode {
        NO_CACHE,
        // Not valid for atomicReplace open.
        TAIL_CACHE,
        // Memory is held until close() is called.
        FULL_CACHE
    }

    long pendingFsyncBytes = 0;
    final boolean writeMode;
    volatile CacheMode cacheMode = CacheMode.NO_CACHE;
    volatile Runnable onClose = () -> {};
    volatile boolean closed = false;

    FileCacheEntry cacheEntry = null;

    FileCacheEntry getCacheEntry() {
        return cacheEntry;
    }

    abstract FileChannel currentWriteChannel();

    abstract String identifier();

    AbstractStorageFile(boolean writeMode) {
        this.writeMode = writeMode;
    }
}
