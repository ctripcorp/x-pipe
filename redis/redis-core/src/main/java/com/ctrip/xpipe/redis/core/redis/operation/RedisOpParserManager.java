package com.ctrip.xpipe.redis.core.redis.operation;

/**
 * @author lishanglin
 * date 2022/2/17
 */
public interface RedisOpParserManager {

    void registerParser(RedisOpType opType, RedisOpParser parser);

    RedisOpParser findParser(RedisOpType opType);

}
