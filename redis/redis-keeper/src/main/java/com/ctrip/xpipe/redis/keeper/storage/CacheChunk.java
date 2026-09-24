package com.ctrip.xpipe.redis.keeper.storage;

import io.netty.buffer.ByteBuf;

final class CacheChunk {

    final ByteBuf buffer;
    volatile long lastAppendNanos = 0;

    CacheChunk(ByteBuf buffer) {
        this.buffer = buffer;
    }
}
