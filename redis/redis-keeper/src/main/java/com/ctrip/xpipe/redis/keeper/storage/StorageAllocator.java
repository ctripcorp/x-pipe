package com.ctrip.xpipe.redis.keeper.storage;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;

class StorageAllocator {
    static ByteBufAllocator ALLOC = PooledByteBufAllocator.DEFAULT;
}
