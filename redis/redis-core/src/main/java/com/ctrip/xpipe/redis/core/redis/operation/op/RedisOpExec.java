package com.ctrip.xpipe.redis.core.redis.operation.op;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.redis.core.redis.operation.RedisSingleKeyOp;

/**
 * @author lishanglin
 * date 2022/2/19
 */
public class RedisOpExec extends AbstractRedisSingleKeyOp implements RedisSingleKeyOp {

    public RedisOpExec(byte[][] rawArgs) {
        super(rawArgs, null, null);
    }

    @Override
    public RedisOpType getOpType() {
        return RedisOpType.EXEC;
    }
}
