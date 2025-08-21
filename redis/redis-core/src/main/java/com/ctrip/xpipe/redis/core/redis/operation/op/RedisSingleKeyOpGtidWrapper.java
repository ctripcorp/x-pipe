package com.ctrip.xpipe.redis.core.redis.operation.op;

import com.ctrip.xpipe.redis.core.redis.operation.RedisKey;
import com.ctrip.xpipe.redis.core.redis.operation.RedisSingleKeyOp;

/**
 * @author lishanglin
 * date 2022/2/18
 */
public class RedisSingleKeyOpGtidWrapper extends AbstractRedisOpGtidWrapper implements RedisSingleKeyOp {

    private RedisSingleKeyOp innerRedisSingleKeyOp;

    public RedisSingleKeyOpGtidWrapper(byte[][] rawGtidArgs, String gtid, RedisSingleKeyOp innerRedisSingleKeyOp) {
        super(rawGtidArgs, gtid, innerRedisSingleKeyOp);
        this.innerRedisSingleKeyOp = innerRedisSingleKeyOp;
    }

    @Override
    public RedisKey getKey() {
        return innerRedisSingleKeyOp.getKey();
    }

    @Override
    public byte[] getValue() {
        return innerRedisSingleKeyOp.getValue();
    }
}
