package com.ctrip.xpipe.redis.core.redis.operation.parser;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParserManager;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static com.ctrip.xpipe.redis.core.redis.operation.parser.RedisOpGtidParser.KEY_GTID;

/**
 * @author lishanglin
 * date 2022/2/17
 */
@Component
public class GeneralRedisOpParser extends AbstractRedisOpParser implements RedisOpParser {

    private RedisOpParserManager parserManager;

    private RedisOpGtidParser gtidParser;

    @Autowired
    public GeneralRedisOpParser(RedisOpParserManager redisOpParserManager) {
        this.parserManager = redisOpParserManager;
        this.gtidParser = new RedisOpGtidParser(redisOpParserManager);
    }

    @Override
    public RedisOp parse(byte[][] args) {
        if (0 == args.length) throw new IllegalArgumentException("illegal empty args");
        String cmd = bytes2Str(args[0]);
        boolean attachGtid = KEY_GTID.equalsIgnoreCase(cmd);

        if (attachGtid) {
            return gtidParser.parse(args);
        } else {
            RedisOpType redisOpType = RedisOpType.lookup(cmd);
            if (!redisOpType.checkArgcNotStrictly(args)) {
                throw new IllegalArgumentException("wrong number of args for " + cmd);
            }

            RedisOpParser parser = parserManager.findParser(redisOpType);
            if (null == parser) throw new UnsupportedOperationException("no parser for " + cmd);
            return parser.parse(args);
        }
    }

    @Override
    public int getOrder() {
        return 0;
    }
}
