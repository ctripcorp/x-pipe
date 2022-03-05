package com.ctrip.xpipe.redis.core.redis.parser;

import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParserManager;
import com.ctrip.xpipe.redis.core.redis.operation.parser.*;
import org.junit.Before;

/**
 * @author lishanglin
 * date 2022/2/22
 */
public class AbstractRedisOpParserTest extends AbstractRedisTest {

    protected RedisOpParserManager redisOpParserManager;

    protected RedisOpParser parser;

    @Before
    public void setupAbstractRedisOpParserTest() {
        redisOpParserManager = new DefaultRedisOpParserManager();
        parser = new GeneralRedisOpParser(redisOpParserManager);
        new RedisOpSetParser(redisOpParserManager);
        new RedisOpMsetParser(redisOpParserManager);
        new RedisOpDelParser(redisOpParserManager);
        new RedisOpSelectParser(redisOpParserManager);
        new RedisOpPingParser(redisOpParserManager);
        new RedisOpPublishParser(redisOpParserManager);
        new RedisOpMultiParser(redisOpParserManager);
    }

}
