package com.ctrip.xpipe.redis.core.redis.operation.transfer;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOpCrdtTransfer;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.tuple.Pair;

/**
 * @author hailu
 * @date 2024/6/11 20:46
 */
public class RedisOpCrdtZIncrbyTransfer implements RedisOpCrdtTransfer {

    private static RedisOpCrdtZIncrbyTransfer instance = new RedisOpCrdtZIncrbyTransfer();

    public static RedisOpCrdtZIncrbyTransfer getInstance() {
        return instance;
    }

    @Override
    public Pair<RedisOpType, byte[][]> transformCrdtRedisOp(RedisOpType redisOpType, byte[][] args) {
        if (args.length < 8) {
            throw new IllegalArgumentException("Invalid CRDT.ZINCRBY command");
        }
        // "CRDT.Zincrby" "hailusortedset" "1" "1717742949954" "1:6" "hello" "2:20" "80" -> "ZADD" "hailusortedset" "80" "hello"
        byte[][] commonArgs = new byte[args.length - 4][];
        commonArgs[0] = RedisOpType.ZADD.name().getBytes();
        commonArgs[1] = args[1];
        commonArgs[2] = args[args.length - 1];
        commonArgs[3] = args[5];
        return Pair.of(RedisOpType.ZADD, commonArgs);
    }
}
