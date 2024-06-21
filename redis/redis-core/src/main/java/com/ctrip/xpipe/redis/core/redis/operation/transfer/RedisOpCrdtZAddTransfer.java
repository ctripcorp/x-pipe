package com.ctrip.xpipe.redis.core.redis.operation.transfer;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOpCrdtTransfer;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.tuple.Pair;

/**
 * @author hailu
 * @date 2024/6/11 20:46
 */
public class RedisOpCrdtZAddTransfer implements RedisOpCrdtTransfer {

    private static RedisOpCrdtZAddTransfer instance = new RedisOpCrdtZAddTransfer();

    public static RedisOpCrdtZAddTransfer getInstance() {
        return instance;
    }

    @Override
    public Pair<RedisOpType, byte[][]> transformCrdtRedisOp(RedisOpType redisOpType, byte[][] args) {
        // "CRDT.ZADD" "hailusortedset" "5" "1706183768965" "5:28" "hi" "2:70" "world" "2:90" -> "ZADD" "hailusortedset" "70" "hi" "90" "world"
        byte[][] commonArgs = new byte[args.length - 3][];
        commonArgs[0] = RedisOpType.ZADD.name().getBytes();
        commonArgs[1] = args[1];
        for (int i = 5; i < args.length; i++) {
            if (i % 2 == 0) {
                byte[] score = extractScore(args[i]);
                if (score == null) {
                    return Pair.of(RedisOpType.UNKNOWN, args);
                }
                commonArgs[i - 4] = score;
            } else {
                commonArgs[i - 2] = args[i];
            }
        }
        return Pair.of(RedisOpType.ZADD, commonArgs);
    }

    private byte[] extractScore(byte[] arg) {
        String value = new String(arg);
        String[] split = value.split(":");
        if (split.length != 2) {
            return null;
        }
        return split[1].getBytes();
    }
}
