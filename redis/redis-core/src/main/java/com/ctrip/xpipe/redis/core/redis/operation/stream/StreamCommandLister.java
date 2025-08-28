package com.ctrip.xpipe.redis.core.redis.operation.stream;

import io.netty.buffer.ByteBuf;

import java.io.IOException;

public interface StreamCommandLister {

    public void onCommand(Object[] payload, ByteBuf commandBuf) throws IOException;
}
