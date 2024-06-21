package com.ctrip.xpipe.redis.core.redis.operation.transfer;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOpCrdtTransfer;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.tuple.Pair;

/**
 * @author hailu
 * @date 2024/6/3 19:07
 */
public class RedisOpCrdtSetTransfer implements RedisOpCrdtTransfer {
    private static RedisOpCrdtSetTransfer instance = new RedisOpCrdtSetTransfer();

    public static RedisOpCrdtSetTransfer getInstance() {
        return instance;
    }

    @Override
    public Pair<RedisOpType, byte[][]> transformCrdtRedisOp(RedisOpType redisOpType, byte[][] args) {
        // "CRDT.SET" "hailu2001" "hailu" "5" "1706184079769" "2:177847;5:35" "1706184079769" -> "PSETEX" "hailu2001" "5000" "hailu"
        if (!bytes2Str(args[args.length - 1]).equalsIgnoreCase("-1")) {
            byte[][] commonArgs = new byte[4][];
            commonArgs[0] = RedisOpType.PSETEX.name().getBytes();
            commonArgs[1] = args[1];
            commonArgs[3] = args[2];
            Long aLong = Long.valueOf(bytes2Str(args[args.length - 1]));
            long current = System.currentTimeMillis();
            long expire = aLong - current;
            if (expire < 0) {
                return Pair.of(RedisOpType.UNKNOWN, args);
            }
            commonArgs[2] = String.valueOf(expire).getBytes();
            return Pair.of(RedisOpType.PSETEX, commonArgs);
        }
        //"CRDT.SET" "hailu2001" "hailu" "5" "1706184079769" "2:177847;5:35" "-1" -> "SET" "hailu2001" "hailu"
        byte[][] commonArgs = new byte[3][];
        commonArgs[0] = RedisOpType.SET.name().getBytes();
        commonArgs[1] = args[1];
        commonArgs[2] = args[2];
        return Pair.of(RedisOpType.SET, commonArgs);
    }
}
