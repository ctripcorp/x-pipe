package com.ctrip.xpipe.redis.core.redis.operation.transfer;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOpCrdtTransfer;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.tuple.Pair;

/**
 * @author hailu
 * @date 2024/6/5 20:14
 */
public class RedisOpCrdtExecTransfer implements RedisOpCrdtTransfer {
    private static RedisOpCrdtExecTransfer instance = new RedisOpCrdtExecTransfer();

    public static RedisOpCrdtExecTransfer getInstance() {
        return instance;
    }

    @Override
    public Pair<RedisOpType, byte[][]> transformCrdtRedisOp(RedisOpType redisOpType, byte[][] args) {
        // "CRDT.EXEC" "5"  - > "EXEC"
        byte[][] commonArgs = new byte[1][];
        commonArgs[0] = RedisOpType.EXEC.name().getBytes();
        return Pair.of(RedisOpType.EXEC, commonArgs);
    }
}
