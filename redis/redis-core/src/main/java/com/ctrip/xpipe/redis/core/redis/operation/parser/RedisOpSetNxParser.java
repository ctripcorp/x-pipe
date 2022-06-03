package com.ctrip.xpipe.redis.core.redis.operation.parser;

import com.ctrip.xpipe.redis.core.redis.operation.*;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpSet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author lishanglin
 * date 2022/2/19
 */
@Component
public class RedisOpSetNxParser extends AbstractRedisOpParser implements RedisOpParser {

    @Autowired
    public RedisOpSetNxParser(RedisOpParserManager redisOpParserManager) {
        redisOpParserManager.registerParser(RedisOpType.SETNX, this);
    }

    @Override
    public RedisOp parse(byte[][] args) {
        return new RedisOpSet(args, new RedisKey(args[1]), args[2]);
    }

    @Override
    public int getOrder() {
        return 0;
    }

}