package com.ctrip.xpipe.redis.keeper.storage;

import java.nio.channels.FileChannel;

public abstract class AbstractStorageFile {

    public enum OpenMode {
        READ,
        WRITE,
        READ_WRITE;

        boolean canRead() {
            return this != WRITE;
        }

        boolean canWrite() {
            return this != READ;
        }
    }

    public enum CacheMode {
        NO_CACHE,
        // Not valid for atomicReplace open.
        TAIL_CACHE,
        // Memory is held until close() is called.
        FULL_CACHE
    }

    volatile long pendingFsyncBytes = 0;
    volatile long lastFsyncNanos = System.nanoTime();
    final OpenMode openMode;
    final boolean atomicReplace;
    volatile CacheMode cacheMode = CacheMode.NO_CACHE;
    volatile Runnable onCacheClose = () -> {};
    volatile boolean cacheClosed = false;
    volatile boolean closed = false;
    long position = 0;

    FileCacheEntry cacheEntry = null;

    FileCacheEntry getCacheEntry() {
        return cacheEntry;
    }

    boolean canRead() {
        return openMode.canRead();
    }

    boolean canWrite() {
        return openMode.canWrite();
    }

    abstract FileChannel currentWriteChannel();

    abstract void openCurrentChannel() throws java.io.IOException;

    abstract void reopenCurrentChannel() throws java.io.IOException;

    abstract String getKey();

    AbstractStorageFile(OpenMode openMode, boolean atomicReplace) {
        this.openMode = openMode;
        this.atomicReplace = atomicReplace;
    }
}
