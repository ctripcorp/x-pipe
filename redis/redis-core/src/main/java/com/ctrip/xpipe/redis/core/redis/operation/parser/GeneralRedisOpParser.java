package com.ctrip.xpipe.redis.core.redis.operation.parser;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParserManager;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

import static com.ctrip.xpipe.redis.core.redis.operation.parser.RedisOpGtidParser.KEY_GTID;

/**
 * @author lishanglin
 * date 2022/2/17
 */
@Component
public class GeneralRedisOpParser implements RedisOpParser {

    private RedisOpParserManager parserManager;

    private RedisOpGtidParser gtidParser;

    @Autowired
    public GeneralRedisOpParser(RedisOpParserManager redisOpParserManager) {
        this.parserManager = redisOpParserManager;
        this.gtidParser = new RedisOpGtidParser(redisOpParserManager);
    }

    public RedisOp parse(List<String> args) {
        if (args.isEmpty()) throw new IllegalArgumentException("illegal empty args");
        String cmd = args.get(0);
        boolean attachGtid = KEY_GTID.equalsIgnoreCase(cmd);

        if (attachGtid) {
            return gtidParser.parse(args);
        } else {
            RedisOpParser parser = parserManager.findParser(RedisOpType.lookup(cmd));
            if (null == parser) throw new UnsupportedOperationException("no parser for " + cmd);
            return parser.parse(args);
        }
    }

    @Override
    public int getOrder() {
        return 0;
    }
}
