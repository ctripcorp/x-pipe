package com.ctrip.xpipe.redis.core.redis.operation.op;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.redis.core.redis.operation.RedisSingleKeyOp;

import java.util.List;

/**
 * @author lishanglin
 * date 2022/2/18
 */
public class RedisOpPing extends AbstractRedisSingleKeyOp<Void> implements RedisSingleKeyOp<Void> {

    public RedisOpPing(List<String> rawArgs) {
        super(rawArgs, null, null);
    }

    @Override
    public RedisOpType getOpType() {
        return RedisOpType.PING;
    }
}
