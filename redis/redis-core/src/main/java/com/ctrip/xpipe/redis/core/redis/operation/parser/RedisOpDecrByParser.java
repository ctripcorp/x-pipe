package com.ctrip.xpipe.redis.core.redis.operation.parser;

import com.ctrip.xpipe.redis.core.redis.operation.*;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpDecrBy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author lishanglin
 * date 2022/2/19
 */
@Component
public class RedisOpDecrByParser implements RedisOpParser {

    @Autowired
    public RedisOpDecrByParser(RedisOpParserManager redisOpParserManager) {
        redisOpParserManager.registerParser(RedisOpType.DECRBY, this);
    }

    @Override
    public RedisOp parse(List<String> args) {
        return new RedisOpDecrBy(args, new RedisKey(args.get(1)), Long.parseLong(args.get(2)));
    }

    @Override
    public int getOrder() {
        return 0;
    }

}
