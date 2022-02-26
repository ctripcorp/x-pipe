package com.ctrip.xpipe.redis.core.redis.operation.parser;

import com.ctrip.xpipe.redis.core.redis.operation.*;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpIncr;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author lishanglin
 * date 2022/2/19
 */
@Component
public class RedisOpIncrParser implements RedisOpParser {

    @Autowired
    public RedisOpIncrParser(RedisOpParserManager redisOpParserManager) {
        redisOpParserManager.registerParser(RedisOpType.INCR, this);
    }

    @Override
    public RedisOp parse(List<String> args) {
        return new RedisOpIncr(args, new RedisKey(args.get(1)));
    }

    @Override
    public int getOrder() {
        return 0;
    }

}
