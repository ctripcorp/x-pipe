package com.ctrip.xpipe.redis.core.redis.operation.transfer;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOpCrdtTransfer;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.tuple.Pair;

/**
 * @author hailu
 * @date 2024/6/11 20:46
 */
public class RedisOpCrdtHSetTransfer implements RedisOpCrdtTransfer {

    private static RedisOpCrdtHSetTransfer instance = new RedisOpCrdtHSetTransfer();

    public static RedisOpCrdtHSetTransfer getInstance() {
        return instance;
    }

    @Override
    public Pair<RedisOpType, byte[][]> transformCrdtRedisOp(RedisOpType redisOpType, byte[][] args) {
        // "CRDT.HSET" "hailu1937" "5" "1706182703266" "2:177847;5:7" "4" "hailutest1" "hailu" "hailutest2" "hailu" -> "hset" "hailu1937" "hailutest1" "hailu" "hailutest2" "hailu"
        if (args.length < 8) {
            return Pair.of(RedisOpType.UNKNOWN, args);
        }
        byte[][] commonArgs = new byte[args.length - 4][];
        commonArgs[0] = RedisOpType.HSET.name().getBytes();
        commonArgs[1] = args[1];
        System.arraycopy(args, 6, commonArgs, 2, args.length - 6);
        return Pair.of(RedisOpType.HSET, commonArgs);
    }
}
