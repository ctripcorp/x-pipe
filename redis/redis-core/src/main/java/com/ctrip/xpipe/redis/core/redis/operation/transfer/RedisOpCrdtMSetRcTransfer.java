package com.ctrip.xpipe.redis.core.redis.operation.transfer;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOpCrdtTransfer;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.tuple.Pair;

/**
 * @author hailu
 * @date 2024/6/4 14:17
 */
public class RedisOpCrdtMSetRcTransfer implements RedisOpCrdtTransfer {

    private static RedisOpCrdtMSetRcTransfer instance = new RedisOpCrdtMSetRcTransfer();

    public static RedisOpCrdtMSetRcTransfer getInstance() {
        return instance;
    }

    @Override
    public Pair<RedisOpType, byte[][]> transformCrdtRedisOp(RedisOpType redisOpType, byte[][] args) {
        if (args.length % 3 != 0) {
            return Pair.of(RedisOpType.UNKNOWN, args);
        }
        // "CRDT.MSET_RC" "5" "1718107088036" "hailutest" "3:1:6" "1:1503" "hailutest1" "3:1:7" "1:1503" "hailutest2" "3:1:8" "1:1503" -> "MSET" "hailutest" "6" "hailutest1" "7" "hailutest2" "8"
        byte[][] commonArgs = new byte[(args.length / 3) * 2 - 1][];
        commonArgs[0] = RedisOpType.MSET.name().getBytes();
        for (int i = 3; i < args.length; i++) {
            if (i % 3 == 0) {
                commonArgs[i / 3 * 2 - 1] = args[i];
            }
            if (i % 3 == 1) {
                byte[] value = extractValue(args[i]);
                if (value == null) {
                    return Pair.of(RedisOpType.UNKNOWN, args);
                }
                commonArgs[i / 3 * 2] = value;
            }
        }
        return Pair.of(RedisOpType.MSET, commonArgs);
    }
}
