package com.ctrip.xpipe.redis.core.redis.operation.stream;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.List;

public interface StreamTransactionListener {

    boolean preAppend(String gtid, long offset) throws IOException;

    int postAppend(ByteBuf commandBuf, Object[] payload) throws IOException;

    int batchPostAppend(List<ByteBuf> commandBufs, List<Object[]> payloads) throws IOException;

    boolean checkOffset(long offset);

}
