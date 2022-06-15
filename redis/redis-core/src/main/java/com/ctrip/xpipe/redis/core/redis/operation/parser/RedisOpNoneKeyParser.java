package com.ctrip.xpipe.redis.core.redis.operation.parser;


import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpNoneKey;

/**
 * @author ayq
 * <p>
 * 2022/6/12 14:17
 */
public class RedisOpNoneKeyParser extends AbstractRedisOpParser implements RedisOpParser {

    private RedisOpType redisOpType;

    public RedisOpNoneKeyParser(RedisOpType redisOpType) {
        this.redisOpType = redisOpType;
    }

    @Override
    public RedisOp parse(byte[][] args) {
        return new RedisOpNoneKey(redisOpType, args);
    }

    @Override
    public int getOrder() {
        return 0;
    }
}
