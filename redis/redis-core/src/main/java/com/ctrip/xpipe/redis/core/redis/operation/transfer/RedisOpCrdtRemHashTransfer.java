package com.ctrip.xpipe.redis.core.redis.operation.transfer;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOpCrdtTransfer;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.tuple.Pair;

/**
 * @author hailu
 * @date 2024/6/11 20:46
 */
public class RedisOpCrdtRemHashTransfer implements RedisOpCrdtTransfer {

    private static RedisOpCrdtRemHashTransfer instance = new RedisOpCrdtRemHashTransfer();

    public static RedisOpCrdtRemHashTransfer getInstance() {
        return instance;
    }

    @Override
    public Pair<RedisOpType, byte[][]> transformCrdtRedisOp(RedisOpType redisOpType, byte[][] args) {
        // "CRDT.REM_HASH" "hailu1937" "1" "1718161692326" "1:1513;2:1500" "hailutest1" "hailutest2" -> "hdel" "hailu1937" "hailutest1" "hailutest2"
        byte[][] commonArgs = new byte[args.length - 3][];
        commonArgs[0] = RedisOpType.HDEL.name().getBytes();
        commonArgs[1] = args[1];
        System.arraycopy(args, 5, commonArgs, 2, args.length - 5);
        return Pair.of(RedisOpType.HDEL, commonArgs);
    }
}
