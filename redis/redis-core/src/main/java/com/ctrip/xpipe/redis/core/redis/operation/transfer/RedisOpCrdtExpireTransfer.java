package com.ctrip.xpipe.redis.core.redis.operation.transfer;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOpCrdtTransfer;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.tuple.Pair;

/**
 * @author hailu
 * @date 2024/6/11 20:46
 */
public class RedisOpCrdtExpireTransfer implements RedisOpCrdtTransfer {

    private static RedisOpCrdtExpireTransfer instance = new RedisOpCrdtExpireTransfer();

    public static RedisOpCrdtExpireTransfer getInstance() {
        return instance;
    }

    @Override
    public Pair<RedisOpType, byte[][]> transformCrdtRedisOp(RedisOpType redisOpType, byte[][] args) {
        // "CRDT.EXPIRE" "hailusortedset" "1" "1718170090362" "1718170590362" "4" - > "PEXPIREAT" "hailusortedset" 1718170590362
        byte[][] commonArgs = new byte[3][];
        commonArgs[0] = RedisOpType.PEXPIREAT.name().getBytes();
        commonArgs[1] = args[1];
        commonArgs[2] = args[4];
        return Pair.of(RedisOpType.PEXPIREAT, commonArgs);
    }
}
