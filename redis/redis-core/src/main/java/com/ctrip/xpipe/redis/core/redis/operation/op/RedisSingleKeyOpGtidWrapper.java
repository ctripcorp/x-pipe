package com.ctrip.xpipe.redis.core.redis.operation.op;

import com.ctrip.xpipe.redis.core.redis.operation.*;

/**
 * @author lishanglin
 * date 2022/2/18
 */
public class RedisSingleKeyOpGtidWrapper extends AbstractRedisOpGtidWrapper implements RedisSingleKeyOp {

    private RedisSingleKeyOp innerRedisSingleKeyOp;

    public RedisSingleKeyOpGtidWrapper(byte[][] rawGtidArgs, String gtid,String dbid, RedisSingleKeyOp innerRedisSingleKeyOp) {
        super(rawGtidArgs, gtid, dbid,innerRedisSingleKeyOp);
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
