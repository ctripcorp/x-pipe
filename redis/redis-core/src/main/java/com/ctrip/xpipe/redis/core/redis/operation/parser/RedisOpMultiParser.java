package com.ctrip.xpipe.redis.core.redis.operation.parser;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParserManager;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpMulti;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author lishanglin
 * date 2022/2/19
 */
@Component
public class RedisOpMultiParser extends AbstractRedisOpParser implements RedisOpParser {

    @Autowired
    public RedisOpMultiParser(RedisOpParserManager redisOpParserManager) {
        redisOpParserManager.registerParser(RedisOpType.MULTI, this);
    }

    @Override
    public RedisOp parse(byte[][] args) {
        return new RedisOpMulti(args);
    }

    @Override
    public int getOrder() {
        return 0;
    }

}
