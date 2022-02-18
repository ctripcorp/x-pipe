package com.ctrip.xpipe.redis.core.redis.operation.op;

import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.redis.operation.*;

/**
 * @author lishanglin
 * date 2022/2/18
 */
public class RedisSingleKeyOpGtidWrapper<T> extends AbstractRedisOpGtidWrapper implements RedisSingleKeyOp<T> {

    private RedisSingleKeyOp<T> innerRedisSingleKeyOp;

    public RedisSingleKeyOpGtidWrapper(GtidSet gtidSet, RedisSingleKeyOp<T> innerRedisSingleKeyOp) {
        super(gtidSet, innerRedisSingleKeyOp);
        this.innerRedisSingleKeyOp = innerRedisSingleKeyOp;
    }

    @Override
    public RedisKey getKey() {
        return innerRedisSingleKeyOp.getKey();
    }

    @Override
    public T getValue() {
        return innerRedisSingleKeyOp.getValue();
    }
}
