package com.ctrip.xpipe.redis.core.redis.operation.transfer;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOpCrdtTransfer;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.tuple.Pair;

/**
 * @author hailu
 * @date 2024/6/5 20:14
 */
public class RedisOpCrdtMultiTransfer implements RedisOpCrdtTransfer {
    private static RedisOpCrdtMultiTransfer instance = new RedisOpCrdtMultiTransfer();

    public static RedisOpCrdtMultiTransfer getInstance() {
        return instance;
    }

    @Override
    public Pair<RedisOpType, byte[][]> transformCrdtRedisOp(RedisOpType redisOpType, byte[][] args) {
        // "CRDT.MULTI" "2" - > "MULTI"
        byte[][] commonArgs = new byte[1][];
        commonArgs[0] = RedisOpType.MULTI.name().getBytes();
        return Pair.of(RedisOpType.MULTI, commonArgs);
    }
}
