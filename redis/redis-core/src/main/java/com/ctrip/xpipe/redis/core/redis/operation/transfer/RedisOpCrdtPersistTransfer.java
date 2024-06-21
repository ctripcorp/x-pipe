package com.ctrip.xpipe.redis.core.redis.operation.transfer;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOpCrdtTransfer;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.tuple.Pair;

/**
 * @author hailu
 * @date 2024/6/11 20:46
 */
public class RedisOpCrdtPersistTransfer implements RedisOpCrdtTransfer {

    private static RedisOpCrdtPersistTransfer instance = new RedisOpCrdtPersistTransfer();

    public static RedisOpCrdtPersistTransfer getInstance() {
        return instance;
    }

    @Override
    public Pair<RedisOpType, byte[][]> transformCrdtRedisOp(RedisOpType redisOpType, byte[][] args) {
        // "CRDT.persist" "hailusortedset" "1" "4" - > "persist " "hailusortedset"
        byte[][] commonArgs = new byte[2][];
        commonArgs[0] = RedisOpType.PERSIST.name().getBytes();
        commonArgs[1] = args[1];
        return Pair.of(RedisOpType.PERSIST, commonArgs);
    }
}
