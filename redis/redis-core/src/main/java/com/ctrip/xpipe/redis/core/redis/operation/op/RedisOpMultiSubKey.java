package com.ctrip.xpipe.redis.core.redis.operation.op;

import com.ctrip.xpipe.redis.core.redis.operation.RedisKey;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import java.util.List;

/**
 * @author TB
 * <p>
 * 2025/10/10 16:43
 */
public class RedisOpMultiSubKey extends AbstractRedisMultiSubKeyOp {
    private RedisOpType redisOpType;

    public RedisOpMultiSubKey(RedisOpType redisOpType, byte[][] rawArgs, RedisKey redisKey,List<RedisKey> subKeys) {
        super(rawArgs,redisKey, subKeys);
        this.redisOpType = redisOpType;
    }


    @Override
    public RedisOpType getOpType() {
        return this.redisOpType;
    }
}
