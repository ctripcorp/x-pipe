package com.ctrip.xpipe.redis.core.redis.operation.transfer;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOpCrdtTransfer;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.tuple.Pair;

/**
 * @author hailu
 * @date 2024/6/11 19:47
 */
public class RedisOpCrdtRcTransfer implements RedisOpCrdtTransfer {

    private static RedisOpCrdtRcTransfer instance = new RedisOpCrdtRcTransfer();

    public static RedisOpCrdtRcTransfer getInstance() {
        return instance;
    }

    @Override
    public Pair<RedisOpType, byte[][]> transformCrdtRedisOp(RedisOpType redisOpType, byte[][] args) {
        // "CRDT.rc" "hailu1945" "5" "1706183161935" "5:13" "3:1:1" "1706184079769" -> "PSETEX" "hailu1945" "5000" "1"
        if (!bytes2Str(args[args.length - 1]).equalsIgnoreCase("-1")) {
            byte[][] commonArgs = new byte[4][];
            commonArgs[0] = RedisOpType.PSETEX.name().getBytes();
            commonArgs[1] = args[1];
            byte[] value = extractValue(args[5]);
            if (value == null) {
                return Pair.of(RedisOpType.UNKNOWN, args);
            }
            commonArgs[3] = value;
            Long aLong = Long.valueOf(bytes2Str(args[args.length - 1]));
            long current = System.currentTimeMillis();
            long expire = aLong - current;
            if (expire < 0) {
                return Pair.of(RedisOpType.UNKNOWN, args);
            }
            commonArgs[2] = String.valueOf(expire).getBytes();
            return Pair.of(RedisOpType.PSETEX, commonArgs);
        }
        // "CRDT.rc" "hailu1945" "5" "1706183161935" "5:13" "3:1:1" "-1" -> "SET" "hailu1945" "1"
        byte[][] commonArgs = new byte[3][];
        commonArgs[0] = RedisOpType.SET.name().getBytes();
        commonArgs[1] = args[1];
        byte[] value = extractValue(args[5]);
        if (value == null) {
            return Pair.of(RedisOpType.UNKNOWN, args);
        }
        commonArgs[2] = value;
        return Pair.of(RedisOpType.SET, commonArgs);
    }
}
