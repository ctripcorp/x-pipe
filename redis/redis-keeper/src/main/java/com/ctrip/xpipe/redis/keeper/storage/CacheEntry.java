package com.ctrip.xpipe.redis.keeper.storage;

final class CacheEntry {
    int refCount;
    Object cache;
}
