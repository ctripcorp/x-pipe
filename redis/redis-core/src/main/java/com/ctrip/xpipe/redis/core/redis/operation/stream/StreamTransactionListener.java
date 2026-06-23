package com.ctrip.xpipe.redis.core.redis.operation.stream;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpItem;
import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.List;

public interface StreamTransactionListener {

    boolean preAppend(String gtid, long offset) throws IOException;

    int postAppend(ByteBuf commandBuf, RedisOpItem redisOpItem) throws IOException;

    int batchPostAppend(List<ByteBuf> commandBufs, List<RedisOpItem> payloads) throws IOException;

    boolean checkOffset(long offset);

    RedisOpParser getOpParser();

    /** 一段已写入的非 GTID 字节区间。 */
    default void onNonGtidWritten(long offset, int length) throws IOException {}

    /** 一段已写入的 GTID 字节区间（含整笔 MULTI 事务）。 */
    default void onGtidWritten(long offset, int length) throws IOException {}

}
