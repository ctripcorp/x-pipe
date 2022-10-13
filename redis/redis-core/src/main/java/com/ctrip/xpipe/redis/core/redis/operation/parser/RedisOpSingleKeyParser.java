package com.ctrip.xpipe.redis.core.redis.operation.parser;

import com.ctrip.xpipe.redis.core.redis.operation.*;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpSingleKey;

/**
 * @author ayq
 * <p>
 * 2022/6/6 12:20
 */
public class RedisOpSingleKeyParser extends AbstractRedisOpParser implements RedisOpParser {

    private RedisOpType redisOpType;
    private Integer keyIndex;
    private Integer valueIndex;

    public RedisOpSingleKeyParser(RedisOpType redisOpType, Integer keyIndex, Integer valueIndex) {
        this.keyIndex = keyIndex;
        this.valueIndex = valueIndex;
        this.redisOpType = redisOpType;
    }

    @Override
    public RedisOp parse(byte[][] args) {
        RedisKey redisKey = keyIndex == null? null: new RedisKey(args[keyIndex]);
        byte[] redisValue = valueIndex == null? null: args[valueIndex];
        return new RedisOpSingleKey(redisOpType, args, redisKey, redisValue);
    }

    @Override
    public int getOrder() {
        return 0;
    }
}
