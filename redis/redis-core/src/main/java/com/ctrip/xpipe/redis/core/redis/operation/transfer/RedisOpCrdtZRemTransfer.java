package com.ctrip.xpipe.redis.core.redis.operation.transfer;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOpCrdtTransfer;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.tuple.Pair;

/**
 * @author hailu
 * @date 2024/6/11 20:46
 */
public class RedisOpCrdtZRemTransfer implements RedisOpCrdtTransfer {

    private static RedisOpCrdtZRemTransfer instance = new RedisOpCrdtZRemTransfer();

    public static RedisOpCrdtZRemTransfer getInstance() {
        return instance;
    }

    @Override
    public Pair<RedisOpType, byte[][]> transformCrdtRedisOp(RedisOpType redisOpType, byte[][] args) {
        // "CRDT.Zrem" "hailusortedset" "5" "1706183987080" "5:34" "3:5:hello" "3:5:world" -> "ZREM" "hailusortedset" "hello" "world"
        byte[][] commonArgs = new byte[args.length - 3][];
        commonArgs[0] = RedisOpType.ZREM.name().getBytes();
        commonArgs[1] = args[1];
        for (int i = 5; i < args.length; i++) {
            byte[] value = extractValue(args[i]);
            if (value == null) {
                return Pair.of(RedisOpType.UNKNOWN, args);
            }
            commonArgs[i - 3] = value;
        }
        return Pair.of(RedisOpType.ZREM, commonArgs);
    }
}
