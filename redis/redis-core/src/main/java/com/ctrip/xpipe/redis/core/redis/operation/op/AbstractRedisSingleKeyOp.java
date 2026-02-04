package com.ctrip.xpipe.redis.core.redis.operation.op;

import com.ctrip.xpipe.redis.core.redis.operation.RedisKey;
import com.ctrip.xpipe.redis.core.redis.operation.RedisSingleKeyOp;

/**
 * @author lishanglin
 * date 2022/2/17
 */
public abstract class AbstractRedisSingleKeyOp extends AbstractRedisOp implements RedisSingleKeyOp {

    private RedisKey key;

    private byte[] value;

    protected boolean isLastOp;

    public AbstractRedisSingleKeyOp(byte[][] rawArgs, RedisKey redisKey, byte[] redisValue) {
        super(rawArgs);
        this.key = redisKey;
        this.value = redisValue;
    }

    public AbstractRedisSingleKeyOp(byte[][] rawArgs, RedisKey redisKey, byte[] redisValue, String gtid) {
        super(rawArgs, gtid);
        this.key = redisKey;
        this.value = redisValue;
    }

    public AbstractRedisSingleKeyOp(byte[][] rawArgs, RedisKey redisKey, byte[] redisValue, String gid, Long timestamp) {
        super(rawArgs, gid, timestamp);
        this.key = redisKey;
        this.value = redisValue;
    }

    public AbstractRedisSingleKeyOp(byte[][] rawArgs, RedisKey redisKey, byte[] redisValue, String gtid, String gid, Long timestamp) {
        super(rawArgs, gtid, gid, timestamp);
        this.key = redisKey;
        this.value = redisValue;
    }

    @Override
    public RedisKey getKey() {
        return key;
    }

    @Override
    public byte[] getValue() {
        return value;
    }

    @Override
    public boolean isLastOp(){
        return isLastOp;
    }

}
