package com.ctrip.xpipe.redis.core.redis.operation.op;

import com.ctrip.xpipe.redis.core.redis.operation.RedisKey;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.redis.core.redis.operation.RedisSingleKeyOp;

/**
 * @author lishanglin
 * date 2022/2/17
 */
public class RedisOpSet extends AbstractRedisSingleKeyOp implements RedisSingleKeyOp {

    public RedisOpSet(RedisKey key, byte[] value) {
        super(new byte[][] {RedisOpType.SET.name().getBytes(), key.get(), value}, key, value);
    }

    public RedisOpSet(byte[][] rawArgs, RedisKey redisKey, byte[] redisValue) {
        super(rawArgs, redisKey, redisValue);
    }

    public RedisOpSet(byte[][] rawArgs, RedisKey redisKey, byte[] redisValue, String gtid) {
        super(rawArgs, redisKey, redisValue, gtid);
    }

    @Override
    public RedisOpType getOpType() {
        return RedisOpType.SET;
    }
}
