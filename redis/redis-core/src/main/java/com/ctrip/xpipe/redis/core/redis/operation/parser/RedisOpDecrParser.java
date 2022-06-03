package com.ctrip.xpipe.redis.core.redis.operation.parser;

import com.ctrip.xpipe.redis.core.redis.operation.*;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpDecr;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author lishanglin
 * date 2022/2/19
 */
@Component
public class RedisOpDecrParser extends AbstractRedisOpParser implements RedisOpParser {

    @Autowired
    public RedisOpDecrParser(RedisOpParserManager redisOpParserManager) {
        redisOpParserManager.registerParser(RedisOpType.DECR, this);
    }

    @Override
    public RedisOp parse(byte[][] args) {
        return new RedisOpDecr(args, new RedisKey(args[1]));
    }

    @Override
    public int getOrder() {
        return 0;
    }

}
