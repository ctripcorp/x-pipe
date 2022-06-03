package com.ctrip.xpipe.redis.core.redis.operation.op;

import com.ctrip.xpipe.redis.core.redis.operation.RedisKey;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.redis.core.redis.operation.RedisSingleKeyOp;

/**
 * @author lishanglin
 * date 2022/2/19
 */
public class RedisOpIncr extends AbstractRedisSingleKeyOp implements RedisSingleKeyOp {

    public RedisOpIncr(byte[][] rawArgs, RedisKey redisKey) {
        super(rawArgs, redisKey, null);
    }

    public RedisOpIncr(byte[][] rawArgs, RedisKey redisKey, String gtid) {
        super(rawArgs, redisKey, null, gtid);
    }

    @Override
    public RedisOpType getOpType() {
        return RedisOpType.INCR;
    }
}
