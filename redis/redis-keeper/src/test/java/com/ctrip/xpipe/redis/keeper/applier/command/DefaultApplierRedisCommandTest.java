package com.ctrip.xpipe.redis.keeper.applier.command;

import com.ctrip.xpipe.client.redis.AsyncRedisClient;
import com.ctrip.xpipe.client.redis.AsyncRedisClientFactory;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParserManager;
import com.ctrip.xpipe.redis.core.redis.operation.parser.DefaultRedisOpParserManager;
import com.ctrip.xpipe.redis.core.redis.operation.parser.RedisOpSetParser;
import org.assertj.core.util.Lists;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author Slight
 * <p>
 * Feb 26, 2022 8:45 PM
 */
public class DefaultApplierRedisCommandTest {

    private RedisOpParserManager parserManager = new DefaultRedisOpParserManager();

    private RedisOpSetParser parser = new RedisOpSetParser(parserManager);

    private RedisOp redisOp = parser.parse(Lists.newArrayList("SET", "K", "V10"));

    @Test
    public void simple() throws Throwable {
        AsyncRedisClient client = AsyncRedisClientFactory.DEFAULT.getOrCreateClient("DefaultApplierRedisCommandTest");
        DefaultApplierRedisCommand command = new DefaultApplierRedisCommand(client, redisOp);
        command.execute().get();
    }
}