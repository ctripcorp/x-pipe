package com.ctrip.xpipe.redis.core.redis.operation.parser;

import com.ctrip.xpipe.redis.core.redis.operation.*;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpPublish;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author lishanglin
 * date 2022/2/19
 */
@Component
public class RedisOpPublishParser implements RedisOpParser {

    @Autowired
    public RedisOpPublishParser(RedisOpParserManager redisOpParserManager) {
        redisOpParserManager.registerParser(RedisOpType.PUBLISH, this);
    }

    @Override
    public RedisOp parse(List<String> args) {
        return new RedisOpPublish(args, new RedisKey(args.get(1)), args.get(2));
    }

    @Override
    public int getOrder() {
        return 0;
    }

}
