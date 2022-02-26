package com.ctrip.xpipe.redis.core.redis.operation.parser;

import com.ctrip.xpipe.redis.core.redis.operation.*;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpSet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author lishanglin
 * date 2022/2/19
 */
@Component
public class RedisOpSetExParser implements RedisOpParser {

    @Autowired
    public RedisOpSetExParser(RedisOpParserManager redisOpParserManager) {
        redisOpParserManager.registerParser(RedisOpType.SETEX, this);
    }

    @Override
    public RedisOp parse(List<String> args) {
        return new RedisOpSet(args, new RedisKey(args.get(1)), args.get(3));
    }

    @Override
    public int getOrder() {
        return 0;
    }

}
