package com.ctrip.xpipe.redis.core.redis.operation.parser;

import com.ctrip.xpipe.redis.core.redis.operation.*;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpIncrBy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author lishanglin
 * date 2022/2/19
 */
@Component
public class RedisOpIncrByParser extends AbstractRedisOpParser implements RedisOpParser {

    @Autowired
    public RedisOpIncrByParser(RedisOpParserManager redisOpParserManager) {
        redisOpParserManager.registerParser(RedisOpType.INCRBY, this);
    }

    @Override
    public RedisOp parse(byte[][] args) {
        return new RedisOpIncrBy(args, new RedisKey(args[1]), args[2]);
    }

    @Override
    public int getOrder() {
        return 0;
    }

}
