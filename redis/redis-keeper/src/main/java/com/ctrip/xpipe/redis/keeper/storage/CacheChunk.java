package com.ctrip.xpipe.redis.keeper.storage;

import io.netty.buffer.ByteBuf;

final class CacheChunk {

    static final long OPEN = -1L;

    final ByteBuf buffer;
    volatile long closeNanos = OPEN;

    CacheChunk(ByteBuf buffer) {
        this.buffer = buffer;
    }

    int capacity() {
        return buffer.capacity();
    }

    void release() {
        buffer.release();
    }

    void close(long nowNanos) {
        closeNanos = nowNanos;
    }

    void reopen() {
        closeNanos = OPEN;
    }
}
