package com.ctrip.xpipe.redis.core.redis.operation.op;

import com.ctrip.xpipe.redis.core.redis.operation.RedisKey;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.redis.core.redis.operation.RedisSingleKeyOp;

/**
 * @author ayq
 * <p>
 * 2022/6/6 15:08
 */
public class RedisOpSingleKV extends AbstractRedisSingleKeyOp implements RedisSingleKeyOp {

    private RedisOpType redisOpType;

    public RedisOpSingleKV(RedisOpType redisOpType, byte[][] rawArgs, RedisKey redisKey, byte[] redisValue) {
        super(rawArgs, redisKey, redisValue);
        this.redisOpType = redisOpType;
    }

    public RedisOpSingleKV(RedisOpType redisOpType, byte[][] rawArgs, RedisKey redisKey, byte[] redisValue, String gtid) {
        super(rawArgs, redisKey, redisValue, gtid);
        this.redisOpType = redisOpType;
    }

    @Override
    public RedisOpType getOpType() {
        return redisOpType;
    }
}
