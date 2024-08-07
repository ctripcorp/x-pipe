package com.ctrip.xpipe.redis.core.redis.operation.transfer;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOpCrdtTransfer;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.tuple.Pair;

/**
 * @author hailu
 * @date 2024/6/5 20:14
 */
public class RedisOpCrdtSelectTransfer implements RedisOpCrdtTransfer {
    private static RedisOpCrdtSelectTransfer instance = new RedisOpCrdtSelectTransfer();

    public static RedisOpCrdtSelectTransfer getInstance() {
        return instance;
    }

    @Override
    public Pair<RedisOpType, byte[][]> transformCrdtRedisOp(RedisOpType redisOpType, byte[][] args) {
        // "CRDT.SELECT" "5" "0" - > "SELECT" "0"
        byte[][] commonArgs = new byte[2][];
        commonArgs[0] = RedisOpType.SELECT.name().getBytes();
        commonArgs[1] = args[2];
        return Pair.of(RedisOpType.SELECT, commonArgs);
    }
}
