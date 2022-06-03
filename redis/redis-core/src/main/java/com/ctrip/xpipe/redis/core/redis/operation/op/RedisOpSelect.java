package com.ctrip.xpipe.redis.core.redis.operation.op;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.redis.core.redis.operation.RedisSingleKeyOp;

/**
 * @author lishanglin
 * date 2022/2/17
 */
public class RedisOpSelect extends AbstractRedisSingleKeyOp implements RedisSingleKeyOp {

    public RedisOpSelect(byte[][] rawArgs, byte[] redisValue) {
        super(rawArgs, null, redisValue);
    }

    @Override
    public RedisOpType getOpType() {
        return RedisOpType.SELECT;
    }
}
