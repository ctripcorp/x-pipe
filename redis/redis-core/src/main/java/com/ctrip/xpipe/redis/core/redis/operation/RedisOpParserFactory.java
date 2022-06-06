package com.ctrip.xpipe.redis.core.redis.operation;

import com.ctrip.xpipe.redis.core.redis.operation.parser.RedisOpSingleKVEnum;
import com.ctrip.xpipe.redis.core.redis.operation.parser.RedisOpSingleKVParser;

/**
 * @author ayq
 * <p>
 * 2022/6/7 00:57
 */
public class RedisOpParserFactory {

    private static RedisOpParserFactory instance = new RedisOpParserFactory();

    public static RedisOpParserFactory getInstance() {
        return instance;
    }

    private RedisOpParserFactory() {}

    public void registerParsers(RedisOpParserManager redisOpParserManager) {
        registerSingleKV(redisOpParserManager);
    }

    public void registerSingleKV(RedisOpParserManager redisOpParserManager) {
        for (RedisOpSingleKVEnum cmd : RedisOpSingleKVEnum.values()) {
            redisOpParserManager.registerParser(
                    cmd.getRedisOpType(),
                    new RedisOpSingleKVParser(cmd.getRedisOpType(), cmd.getKeyIndex(),cmd.getValueIndex()));
        }
    }
}
