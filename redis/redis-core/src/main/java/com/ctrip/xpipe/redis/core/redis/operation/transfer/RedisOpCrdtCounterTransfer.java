package com.ctrip.xpipe.redis.core.redis.operation.transfer;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOpCrdtTransfer;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.tuple.Pair;

/**
 * @author hailu
 * @date 2024/6/11 20:21
 */
public class RedisOpCrdtCounterTransfer implements RedisOpCrdtTransfer {
    private static RedisOpCrdtCounterTransfer instance = new RedisOpCrdtCounterTransfer();

    public static RedisOpCrdtCounterTransfer getInstance() {
        return instance;
    }

    @Override
    public Pair<RedisOpType, byte[][]> transformCrdtRedisOp(RedisOpType redisOpType, byte[][] args) {
        // "CRDT.COUNTER" "hailu1945" "5" "1706183220461" "5:16" "4" "4:5" "6" ->set hailu1945 6
        if (args.length < 8) {
            return Pair.of(RedisOpType.UNKNOWN, args);
        }
        byte[][] commonArgs = new byte[3][];
        commonArgs[0] = RedisOpType.SET.name().getBytes();
        commonArgs[1] = args[1];
        commonArgs[2] = args[args.length - 1];
        return Pair.of(RedisOpType.SET, commonArgs);
    }
}
