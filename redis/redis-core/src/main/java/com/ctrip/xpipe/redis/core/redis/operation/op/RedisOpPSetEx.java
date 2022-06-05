package com.ctrip.xpipe.redis.core.redis.operation.op;

import com.ctrip.xpipe.redis.core.redis.operation.RedisKey;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.redis.core.redis.operation.RedisSingleKeyOp;

/**
 * @author lishanglin
 * date 2022/2/19
 */
public class RedisOpPSetEx extends AbstractRedisSingleKeyOp implements RedisSingleKeyOp {

    public RedisOpPSetEx(RedisKey redisKey, byte[] redisValue, long expireMilli) {
        super(new byte[][] {RedisOpType.PSETEX.name().getBytes(),
                redisKey.get(), String.valueOf(expireMilli).getBytes(), redisValue},
                redisKey, redisValue);
    }

    public RedisOpPSetEx(byte[][] rawArgs, RedisKey redisKey, byte[] redisValue) {
        super(rawArgs, redisKey, redisValue);
    }

    public RedisOpPSetEx(byte[][] rawArgs, RedisKey redisKey, byte[] redisValue, String gtid) {
        super(rawArgs, redisKey, redisValue, gtid);
    }

    @Override
    public RedisOpType getOpType() {
        return RedisOpType.PSETEX;
    }
}
