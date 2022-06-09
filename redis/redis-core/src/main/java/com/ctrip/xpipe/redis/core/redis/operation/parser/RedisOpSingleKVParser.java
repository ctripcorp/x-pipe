package com.ctrip.xpipe.redis.core.redis.operation.parser;

import com.ctrip.xpipe.redis.core.redis.operation.*;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpSingleKV;

/**
 * @author ayq
 * <p>
 * 2022/6/6 12:20
 */
public class RedisOpSingleKVParser extends AbstractRedisOpParser implements RedisOpParser {

    private RedisOpType redisOpType;
    private Integer keyIndex;
    private Integer valueIndex;

    public RedisOpSingleKVParser(RedisOpType redisOpType, Integer keyIndex, Integer valueIndex) {
        this.keyIndex = keyIndex;
        this.valueIndex = valueIndex;
        this.redisOpType = redisOpType;
    }

    @Override
    public RedisOp parse(byte[][] args) {
        RedisKey redisKey = keyIndex == null? null: new RedisKey(args[keyIndex]);
        byte[] redisValue = valueIndex == null? null: args[valueIndex];
        return new RedisOpSingleKV(redisOpType, args, redisKey, redisValue);
    }

    @Override
    public int getOrder() {
        return 0;
    }
}
