package com.ctrip.xpipe.redis.keeper.storage;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.List;

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
    volatile long lastModified = 0;
    final OpenMode openMode;
    final boolean atomicReplace;
    volatile CacheMode cacheMode = CacheMode.NO_CACHE;
    volatile Runnable onCacheClose = () -> {};
    volatile boolean closed = false;
    volatile boolean needPrepare = false;
    volatile IOException noSpaceFailure;
    long position = 0;

    FileCacheEntry cacheEntry = null;
    final String key;
    final String path;
    final String dirPath;

    // IO key for inFlight serialization.
    final String ioKey;

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

    abstract long openCurrentChannel() throws IOException;

    abstract List<FileChannel> detachCurrentChannels();

    void markNoSpace(IOException failure) {
        if (noSpaceFailure == null) {
            noSpaceFailure = failure;
        }
    }

    void throwIfNoSpace() {
        if (noSpaceFailure != null) {
            throw new StorageIOException(noSpaceFailure);
        }
    }

    String getKey() {
        return key;
    }

    AbstractStorageFile(OpenMode openMode, boolean atomicReplace, String key, String ioKey,
            String path, String dirPath) {
        this.openMode = openMode;
        this.atomicReplace = atomicReplace;
        this.key = key;
        this.ioKey = ioKey;
        this.path = path;
        this.dirPath = dirPath;
    }
}
