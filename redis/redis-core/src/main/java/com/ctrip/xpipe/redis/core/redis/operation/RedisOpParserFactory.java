package com.ctrip.xpipe.redis.core.redis.operation;

import com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpMset;
import com.ctrip.xpipe.redis.core.redis.operation.parser.RedisOpDelParser;
import com.ctrip.xpipe.redis.core.redis.operation.parser.RedisOpMsetParser;
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
        registerMultiKV(redisOpParserManager);
    }

    public void registerSingleKV(RedisOpParserManager redisOpParserManager) {
        for (RedisOpSingleKVEnum cmd : RedisOpSingleKVEnum.values()) {
            redisOpParserManager.registerParser(
                    cmd.getRedisOpType(),
                    new RedisOpSingleKVParser(cmd.getRedisOpType(), cmd.getKeyIndex(),cmd.getValueIndex()));
        }
    }

    public void registerMultiKV(RedisOpParserManager redisOpParserManager) {
        redisOpParserManager.registerParser(RedisOpType.MSET, new RedisOpMsetParser(redisOpParserManager));
        redisOpParserManager.registerParser(RedisOpType.DEL, new RedisOpDelParser(redisOpParserManager));
    }
}
