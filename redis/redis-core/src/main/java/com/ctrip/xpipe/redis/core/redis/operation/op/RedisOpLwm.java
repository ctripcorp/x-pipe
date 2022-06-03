package com.ctrip.xpipe.redis.core.redis.operation.op;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.redis.core.redis.operation.RedisSingleKeyOp;

import java.util.List;

/**
 * @author Slight
 * <p>
 * Jun 01, 2022 07:36
 */
public class RedisOpLwm extends AbstractRedisSingleKeyOp<String> implements RedisSingleKeyOp<String> {

    public RedisOpLwm(List<String> rawArgs) {
        super(rawArgs, null, null);
    }

    @Override
    public RedisOpType getOpType() {
        return RedisOpType.LWM;
    }
}
