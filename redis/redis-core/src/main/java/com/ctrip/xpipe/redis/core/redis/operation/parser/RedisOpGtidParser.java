package com.ctrip.xpipe.redis.core.redis.operation.parser;

import com.ctrip.xpipe.redis.core.redis.operation.*;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisMultiKeyOpGtidWrapper;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisSingleKeyOpGtidWrapper;

import java.util.List;

/**
 * @author lishanglin
 * date 2022/2/17
 */
public class RedisOpGtidParser implements RedisOpParser {

    public static final String KEY_GTID = "GTID";

    private RedisOpParserManager parserManager;

    public RedisOpGtidParser(RedisOpParserManager redisOpParserManager) {
        this.parserManager = redisOpParserManager;
    }

    @Override
    public RedisOp parse(List<String> args) {
        if (args.size() < 3) throw new IllegalArgumentException("no enough args found for gtid");
        String gtid = args.get(1);

        String subCmd = args.get(2);
        RedisOpType subOpType = RedisOpType.lookup(subCmd);
        List<String> subArgs = args.subList(2, args.size());
        if (!subOpType.checkArgcNotStrictly(subArgs)) {
            throw new IllegalArgumentException("wrong number of args for " + subCmd);
        }

        RedisOpParser subParser = parserManager.findParser(subOpType);
        if (null == subParser) throw new UnsupportedOperationException("no parser for " + subCmd);
        RedisOp redisOp = subParser.parse(subArgs);

        if (redisOp instanceof RedisSingleKeyOp) return new RedisSingleKeyOpGtidWrapper<>(gtid, (RedisSingleKeyOp<?>)redisOp);
        else if (redisOp instanceof RedisMultiKeyOp) return new RedisMultiKeyOpGtidWrapper<>(gtid, (RedisMultiKeyOp<?>)redisOp);
        else throw new UnsupportedOperationException("unsupport inner redis op " + redisOp);
    }

    @Override
    public int getOrder() {
        return 0;
    }
}
