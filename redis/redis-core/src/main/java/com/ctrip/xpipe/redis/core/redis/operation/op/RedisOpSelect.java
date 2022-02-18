package com.ctrip.xpipe.redis.core.redis.operation.op;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.redis.core.redis.operation.RedisSingleKeyOp;

import java.util.List;

/**
 * @author lishanglin
 * date 2022/2/17
 */
public class RedisOpSelect extends AbstractRedisSingleKeyOp<Long> implements RedisSingleKeyOp<Long> {

    public RedisOpSelect(List<String> rawArgs, Long redisValue) {
        super(rawArgs, null, redisValue);
    }

    @Override
    public RedisOpType getOpType() {
        return RedisOpType.SELECT;
    }
}
