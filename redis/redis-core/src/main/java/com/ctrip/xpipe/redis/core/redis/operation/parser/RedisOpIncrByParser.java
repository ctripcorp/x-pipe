package com.ctrip.xpipe.redis.core.redis.operation.parser;

import com.ctrip.xpipe.redis.core.redis.operation.*;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpIncrBy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author lishanglin
 * date 2022/2/19
 */
@Component
public class RedisOpIncrByParser implements RedisOpParser {

    @Autowired
    public RedisOpIncrByParser(RedisOpParserManager redisOpParserManager) {
        redisOpParserManager.registerParser(RedisOpType.INCRBY, this);
    }

    @Override
    public RedisOp parse(List<String> args) {
        return new RedisOpIncrBy(args, new RedisKey(args.get(1)), Long.parseLong(args.get(2)));
    }

    @Override
    public int getOrder() {
        return 0;
    }

}
