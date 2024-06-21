package com.ctrip.xpipe.redis.core.redis.operation.transfer;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOpCrdtTransfer;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.tuple.Pair;

/**
 * @author hailu
 * @date 2024/6/11 20:46
 */
public class RedisOpCrdtSremTransfer implements RedisOpCrdtTransfer {

    private static RedisOpCrdtSremTransfer instance = new RedisOpCrdtSremTransfer();

    public static RedisOpCrdtSremTransfer getInstance() {
        return instance;
    }

    @Override
    public Pair<RedisOpType, byte[][]> transformCrdtRedisOp(RedisOpType redisOpType, byte[][] args) {
        // "CRDT.Srem" "hailuset" "5" "1706183434918" "5:28" "hello" "world" -> "SREM" "hailuset" "hello" "world"
        byte[][] commonArgs = new byte[args.length - 3][];
        commonArgs[0] = RedisOpType.SREM.name().getBytes();
        commonArgs[1] = args[1];
        System.arraycopy(args, 5, commonArgs, 2, args.length - 5);
        return Pair.of(RedisOpType.SREM, commonArgs);
    }
}
