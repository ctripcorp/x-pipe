package com.ctrip.xpipe.redis.core.redis.operation.op;

import com.ctrip.xpipe.redis.core.redis.operation.RedisKey;
import com.ctrip.xpipe.redis.core.redis.operation.RedisNoneKeyOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;

/**
 * @author ayq
 * <p>
 * 2022/6/12 15:30
 */
public class RedisOpNoneKey extends AbstractRedisSingleKeyOp implements RedisNoneKeyOp {

    private RedisOpType redisOpType;

    public RedisOpNoneKey(RedisOpType redisOpType, byte[][] rawArgs) {
        super(rawArgs, null, null);
        this.redisOpType = redisOpType;
    }

    public RedisOpNoneKey(RedisOpType redisOpType, byte[][] rawArgs, String gtid) {
        super(rawArgs, null, null, gtid);
        this.redisOpType = redisOpType;
    }

    @Override
    public RedisOpType getOpType() {
        return redisOpType;
    }
}
