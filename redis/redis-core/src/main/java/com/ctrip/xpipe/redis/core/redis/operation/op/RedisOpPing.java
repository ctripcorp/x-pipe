package com.ctrip.xpipe.redis.core.redis.operation.op;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.redis.core.redis.operation.RedisSingleKeyOp;

/**
 * @author lishanglin
 * date 2022/2/18
 */
public class RedisOpPing extends AbstractRedisSingleKeyOp implements RedisSingleKeyOp {

    public RedisOpPing(byte[][] rawArgs) {
        super(rawArgs, null, null);
    }

    @Override
    public RedisOpType getOpType() {
        return RedisOpType.PING;
    }
}
