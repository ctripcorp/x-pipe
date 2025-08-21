package com.ctrip.xpipe.redis.core.redis.operation.parser;

import com.ctrip.xpipe.redis.core.redis.operation.RedisKey;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpSingleKey;
import com.ctrip.xpipe.tuple.Pair;

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
        // conversion to  common redis op
        Pair<RedisOpType, byte[][]> pair = redisOpType.transfer(redisOpType, args);
        args = pair.getValue();
        byte[] redisValue = valueIndex == null ? null : args[valueIndex];
        RedisKey redisKey = keyIndex == null ? null : new RedisKey(args[keyIndex]);
        return new RedisOpSingleKey(pair.getKey(), args, redisKey, redisValue);
    }

    @Override
    public int getOrder() {
        return 0;
    }
}
