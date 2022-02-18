package com.ctrip.xpipe.redis.core.redis.operation.op;

import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.redis.operation.RedisKey;
import com.ctrip.xpipe.redis.core.redis.operation.RedisSingleKeyOp;

import java.util.List;

/**
 * @author lishanglin
 * date 2022/2/17
 */
public abstract class AbstractRedisSingleKeyOp<T> extends AbstractRedisOp implements RedisSingleKeyOp<T> {

    private RedisKey key;

    private T value;

    public AbstractRedisSingleKeyOp(List<String> rawArgs, RedisKey redisKey, T redisValue) {
        super(rawArgs);
        this.key = redisKey;
        this.value = redisValue;
    }

    public AbstractRedisSingleKeyOp(List<String> rawArgs, RedisKey redisKey, T redisValue, GtidSet gtidSet) {
        super(rawArgs, gtidSet);
        this.key = redisKey;
        this.value = redisValue;
    }

    public AbstractRedisSingleKeyOp(List<String> rawArgs, RedisKey redisKey, T redisValue, String gid, Long timestamp) {
        super(rawArgs, gid, timestamp);
        this.key = redisKey;
        this.value = redisValue;
    }

    public AbstractRedisSingleKeyOp(List<String> rawArgs, RedisKey redisKey, T redisValue, GtidSet gtidSet, String gid, Long timestamp) {
        super(rawArgs, gtidSet, gid, timestamp);
        this.key = redisKey;
        this.value = redisValue;
    }

    @Override
    public RedisKey getKey() {
        return key;
    }

    @Override
    public T getValue() {
        return value;
    }

}
