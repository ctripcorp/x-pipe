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
        return new RedisOpSingleKV(redisOpType, args, new RedisKey(args[keyIndex]), args[valueIndex]);
    }

    @Override
    public int getOrder() {
        return 0;
    }
}
