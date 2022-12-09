package com.ctrip.xpipe.redis.core.redis.operation;

import com.ctrip.xpipe.redis.core.redis.operation.parser.*;

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
        registerNoneKey(redisOpParserManager);
        registerSingleKey(redisOpParserManager);
        registerMultiKeys(redisOpParserManager);
    }

    private void registerNoneKey(RedisOpParserManager redisOpParserManager) {
        for (RedisOpNoneKeyEnum cmd : RedisOpNoneKeyEnum.values()) {
            redisOpParserManager.registerParser(
                    cmd.getRedisOpType(),
                    new RedisOpNoneKeyParser(cmd.getRedisOpType()));
        }
    }

    private void registerSingleKey(RedisOpParserManager redisOpParserManager) {
        for (RedisOpSingleKeyEnum cmd : RedisOpSingleKeyEnum.values()) {
            redisOpParserManager.registerParser(
                    cmd.getRedisOpType(),
                    new RedisOpSingleKeyParser(cmd.getRedisOpType(), cmd.getKeyIndex(),cmd.getValueIndex()));
        }
    }

    private void registerMultiKeys(RedisOpParserManager redisOpParserManager) {
        for (RedisOpMultiKeysEnum cmd : RedisOpMultiKeysEnum.values()) {
            redisOpParserManager.registerParser(
                    cmd.getRedisOpType(),
                    new RedisOpMultiKeysParser(cmd.getRedisOpType(), cmd.getKeyStartIndex(), cmd.getKvNum()));
        }
    }
}
