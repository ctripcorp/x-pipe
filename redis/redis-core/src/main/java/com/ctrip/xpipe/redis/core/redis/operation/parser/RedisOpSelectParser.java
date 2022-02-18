package com.ctrip.xpipe.redis.core.redis.operation.parser;

import com.ctrip.xpipe.redis.core.redis.operation.*;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpSelect;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

/**
 * @author lishanglin
 * date 2022/2/18
 */
public class RedisOpSelectParser implements RedisOpParser {

    @Autowired
    public RedisOpSelectParser(RedisOpParserManager redisOpParserManager) {
        redisOpParserManager.registerParser(RedisOpType.SELECT, this);
    }

    @Override
    public RedisOp parse(List<String> args) {
        if (args.size() < 2) throw new IllegalArgumentException("no enough args for select");
        return new RedisOpSelect(args, Long.parseLong(args.get(1)));
    }

    @Override
    public int getOrder() {
        return 0;
    }

}
