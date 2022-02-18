package com.ctrip.xpipe.redis.core.redis.operation.parser;

import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.redis.operation.*;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisMultiKeyOpGtidWrapper;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisSingleKeyOpGtidWrapper;
import com.ctrip.xpipe.utils.StringUtil;

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
        GtidSet gtidSet = new GtidSet(args.get(1));

        String subCmd = args.get(2);
        if (StringUtil.isEmpty(subCmd)) throw new IllegalArgumentException("illegal empty cmd");
        RedisOpParser subParser = parserManager.findParser(RedisOpType.lookup(args.get(2).trim()));
        if (null == subParser) throw new UnsupportedOperationException("no parser for " + subCmd);

        RedisOp redisOp = subParser.parse(args.subList(2, args.size()));

        if (redisOp instanceof RedisSingleKeyOp) return new RedisSingleKeyOpGtidWrapper<>(gtidSet, (RedisSingleKeyOp<?>)redisOp);
        else if (redisOp instanceof RedisMultiKeyOp) return new RedisMultiKeyOpGtidWrapper<>(gtidSet, (RedisMultiKeyOp<?>)redisOp);
        else throw new UnsupportedOperationException("unsupport inner redis op " + redisOp);
    }

    @Override
    public int getOrder() {
        return 0;
    }
}
