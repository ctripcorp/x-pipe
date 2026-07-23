package com.ctrip.xpipe.redis.keeper.storage;

import java.util.concurrent.atomic.AtomicLong;

final class CacheMemoryTracker {

    private final AtomicLong committedBytes = new AtomicLong(0);

    long committedBytes() {
        return committedBytes.get();
    }

    boolean reserve(long bytes, long limitBytes) {
        if (bytes == 0) return true;
        while (true) {
            long current = committedBytes.get();
            long next = current + bytes;
            if (next > limitBytes) {
                return false;
            }
            if (committedBytes.compareAndSet(current, next)) {
                return true;
            }
        }
    }

    void release(long bytes) {
        if (bytes == 0) return;
        committedBytes.addAndGet(-bytes);
    }
}
