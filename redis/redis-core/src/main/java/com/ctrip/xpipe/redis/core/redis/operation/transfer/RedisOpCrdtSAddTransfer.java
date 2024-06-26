package com.ctrip.xpipe.redis.core.redis.operation.transfer;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOpCrdtTransfer;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.tuple.Pair;

/**
 * @author hailu
 * @date 2024/6/11 20:46
 */
public class RedisOpCrdtSAddTransfer implements RedisOpCrdtTransfer {

    private static RedisOpCrdtSAddTransfer instance = new RedisOpCrdtSAddTransfer();

    public static RedisOpCrdtSAddTransfer getInstance() {
        return instance;
    }

    @Override
    public Pair<RedisOpType, byte[][]> transformCrdtRedisOp(RedisOpType redisOpType, byte[][] args) {
        // "CRDT.SADD" "hailuset" "5" "1706183385248" "5:27" "hello" "world" -> "SADD" "hailuset" "hello" "world"
        if (args.length < 6) {
            return Pair.of(RedisOpType.UNKNOWN, args);
        }
        byte[][] commonArgs = new byte[args.length - 3][];
        commonArgs[0] = RedisOpType.SADD.name().getBytes();
        commonArgs[1] = args[1];
        System.arraycopy(args, 5, commonArgs, 2, args.length - 5);
        return Pair.of(RedisOpType.SADD, commonArgs);
    }
}
