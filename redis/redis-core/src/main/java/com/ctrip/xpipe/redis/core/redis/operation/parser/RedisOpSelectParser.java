package com.ctrip.xpipe.redis.core.redis.operation.parser;

import com.ctrip.xpipe.redis.core.redis.operation.*;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpSelect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author lishanglin
 * date 2022/2/18
 */
@Component
public class RedisOpSelectParser extends AbstractRedisOpParser implements RedisOpParser {

    @Autowired
    public RedisOpSelectParser(RedisOpParserManager redisOpParserManager) {
        redisOpParserManager.registerParser(RedisOpType.SELECT, this);
    }

    @Override
    public RedisOp parse(byte[][] args) {
        return new RedisOpSelect(args, args[1]);
    }

    @Override
    public int getOrder() {
        return 0;
    }

}
