package com.ctrip.xpipe.redis.core.redis.operation.transfer;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOpCrdtTransfer;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.tuple.Pair;

/**
 * @author hailu
 * @date 2024/6/4 14:38
 */
public class RedisOpCrdtDelTransfer implements RedisOpCrdtTransfer {
    private static RedisOpCrdtDelTransfer instance = new RedisOpCrdtDelTransfer();
    public static RedisOpCrdtDelTransfer getInstance() {
        return instance;
    }
    @Override
    public Pair<RedisOpType, byte[][]> transformCrdtRedisOp(RedisOpType redisOpType, byte[][] args) {
        if (args.length < 5) {
            return Pair.of(RedisOpType.UNKNOWN, args);
        }
        // "CRDT.DEL_REG" "hailu2002" "5" "1706184168902" "2:177847;5:39" - > "DEL" "hailu2002"
        byte[][] commonArgs = new byte[2][];
        commonArgs[0] = "DEL".getBytes();
        commonArgs[1] = args[1];
        return Pair.of(RedisOpType.DEL, commonArgs);
    }

    @Override
    public RedisOpType getRedisOpType() {
        return RedisOpType.CRDT_DEL_REG;
    }

    @Override
    public int getOrder() {
        return 0;
    }
}
