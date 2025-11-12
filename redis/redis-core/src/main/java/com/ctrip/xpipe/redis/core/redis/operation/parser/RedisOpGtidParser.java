package com.ctrip.xpipe.redis.core.redis.operation.parser;

import com.ctrip.xpipe.redis.core.redis.operation.*;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisMultiKeyOpGtidWrapper;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisMultiSubKeyOpGtidWrapper;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisSingleKeyOpGtidWrapper;

import java.util.Arrays;

/**
 * @author lishanglin
 * date 2022/2/17
 */
public class RedisOpGtidParser extends AbstractRedisOpParser implements RedisOpParser {

    public static final String KEY_GTID = "GTID";

    private RedisOpParserManager parserManager;

    public RedisOpGtidParser(RedisOpParserManager redisOpParserManager) {
        this.parserManager = redisOpParserManager;
    }

    @Override
    // format: GTID 00229094ffbf30b0e016ccb8a9ffe327d560accc:1 0 set k1 v1
    public RedisOp parse(byte[][] args) {
        if (args.length < 4) throw new IllegalArgumentException("no enough args found for gtid");
        String gtid = bytes2Str(args[1]);
        byte[][] gtidArgs = Arrays.copyOfRange(args, 0, 3);
        String dbid = bytes2Str(args[2]);
        String subCmd = bytes2Str(args[3]);
        RedisOpType subOpType = RedisOpType.lookup(subCmd);
        byte[][] subArgs = Arrays.copyOfRange(args, 3, args.length);
        if (!subOpType.checkArgcNotStrictly(subArgs)) {
            throw new IllegalArgumentException("wrong number of args for " + subCmd);
        }

        RedisOpParser subParser = parserManager.findParser(subOpType);
        if (null == subParser) throw new UnsupportedOperationException("no parser for " + subCmd);
        RedisOp redisOp = subParser.parse(subArgs);

        if (redisOp instanceof RedisSingleKeyOp) return new RedisSingleKeyOpGtidWrapper(gtidArgs, gtid,dbid, (RedisSingleKeyOp)redisOp);
        else if (redisOp instanceof RedisMultiKeyOp) return new RedisMultiKeyOpGtidWrapper(gtidArgs, gtid, dbid,(RedisMultiKeyOp)redisOp);
        else if (redisOp instanceof RedisMultiSubKeyOp) return new RedisMultiSubKeyOpGtidWrapper(gtidArgs, gtid, dbid,(RedisMultiSubKeyOp)redisOp);
        else throw new UnsupportedOperationException("unsupport inner redis op " + redisOp);
    }

    @Override
    public int getOrder() {
        return 0;
    }
}
