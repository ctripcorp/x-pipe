package com.ctrip.xpipe.redis.core.redis.operation.op;

import com.ctrip.xpipe.redis.core.redis.operation.RedisKey;
import com.ctrip.xpipe.redis.core.redis.operation.RedisMultiSubKeyOp;

import java.util.List;

/**
 * @author TB
 * <p>
 * 2025/10/10 17:07
 */
public abstract class AbstractRedisMultiSubKeyOp extends AbstractRedisOp implements RedisMultiSubKeyOp {
    private RedisKey redisKey;
    private List<RedisKey> subKeys;
    public AbstractRedisMultiSubKeyOp(byte[][] rawArgs, RedisKey redisKey,List<RedisKey> subKeys) {
        super(rawArgs);
        this.redisKey = redisKey;
        this.subKeys = subKeys;
    }

    @Override
    public RedisKey getKey() {
        return this.redisKey;
    }

    @Override
    public List<RedisKey> getAllSubKeys() {
        return this.subKeys;
    }
}
