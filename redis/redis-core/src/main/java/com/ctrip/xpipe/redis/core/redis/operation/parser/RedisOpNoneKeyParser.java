package com.ctrip.xpipe.redis.core.redis.operation.parser;


import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpNoneKey;
import com.ctrip.xpipe.tuple.Pair;

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
        Pair<RedisOpType, byte[][]> pair = redisOpType.transfer(redisOpType, args);
        args = pair.getValue();
        return new RedisOpNoneKey(pair.getKey(), args);
    }

    @Override
    public int getOrder() {
        return 0;
    }
}
