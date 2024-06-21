package com.ctrip.xpipe.redis.core.redis.operation;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.api.lifecycle.Ordered;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.tuple.Pair;

/**
 * @author hailu
 * @date 2024/6/3 16:10
 */
public interface RedisOpCrdtTransfer {
    Pair<RedisOpType, byte[][]> transformCrdtRedisOp(RedisOpType redisOpType, byte[][] args);

    default String bytes2Str(byte[] arg) {
        return new String(arg, Codec.defaultCharset);
    }

    default byte[] extractValue(byte[] arg) {
        String value = new String(arg);
        String[] split = value.split(":");
        if (split.length != 3) {
            return null;
        }
        return split[2].getBytes();
    }
}
