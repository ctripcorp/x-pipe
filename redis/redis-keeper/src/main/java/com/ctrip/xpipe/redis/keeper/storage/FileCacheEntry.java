package com.ctrip.xpipe.redis.keeper.storage;

final class FileCacheEntry extends CacheEntry {
    // FULL_CACHE only: writer-side initialization may overwrite reader-side preload, but reader-side preload
    // must never overwrite cache contents that were initialized by a writer.
    volatile boolean fullCacheInitializedByWriter = false;
}
