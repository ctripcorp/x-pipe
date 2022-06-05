package com.ctrip.xpipe.redis.core.redis.operation.op;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.redis.core.redis.operation.RedisSingleKeyOp;

/**
 * @author lishanglin
 * date 2022/2/17
 */
public class RedisOpSelect extends AbstractRedisSingleKeyOp implements RedisSingleKeyOp {

    public RedisOpSelect(int dbId) {
        this(new byte[][] {RedisOpType.SELECT.name().getBytes(), String.valueOf(dbId).getBytes()}, null);
    }

    public RedisOpSelect(byte[][] rawArgs, byte[] redisValue) {
        super(rawArgs, null, redisValue);
    }

    @Override
    public RedisOpType getOpType() {
        return RedisOpType.SELECT;
    }
}
