package com.ctrip.xpipe.redis.core.redis.operation;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.api.lifecycle.Ordered;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.tuple.Pair;

/**
 * @author hailu
 * @date 2024/6/3 16:10
 */
public interface RedisOpCrdtTransfer extends Ordered {
    Pair<RedisOpType, byte[][]> transformCrdtRedisOp(RedisOpType redisOpType, byte[][] args);

    RedisOpType getRedisOpType();

    default String bytes2Str(byte[] arg) {
        return new String(arg, Codec.defaultCharset);
    }
}
