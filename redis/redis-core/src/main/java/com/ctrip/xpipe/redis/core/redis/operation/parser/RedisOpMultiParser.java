package com.ctrip.xpipe.redis.core.redis.operation.parser;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParserManager;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpMulti;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author lishanglin
 * date 2022/2/19
 */
@Component
public class RedisOpMultiParser implements RedisOpParser {

    @Autowired
    public RedisOpMultiParser(RedisOpParserManager redisOpParserManager) {
        redisOpParserManager.registerParser(RedisOpType.MULTI, this);
    }

    @Override
    public RedisOp parse(List<String> args) {
        return new RedisOpMulti(args);
    }

    @Override
    public int getOrder() {
        return 0;
    }

}
