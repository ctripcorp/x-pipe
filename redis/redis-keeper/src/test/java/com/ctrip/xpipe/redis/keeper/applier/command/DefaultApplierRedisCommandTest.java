package com.ctrip.xpipe.redis.keeper.applier.command;

import com.ctrip.xpipe.client.redis.AsyncRedisClient;
import com.ctrip.xpipe.client.redis.AsyncRedisClientFactory;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParserManager;
import com.ctrip.xpipe.redis.core.redis.operation.parser.DefaultRedisOpParserManager;
import com.ctrip.xpipe.redis.core.redis.operation.parser.RedisOpSetParser;
import com.ctrip.xpipe.redis.keeper.applier.sequence.ApplierSequenceController;
import com.ctrip.xpipe.redis.keeper.applier.sequence.DefaultSequenceController;
import org.assertj.core.util.Lists;
import org.junit.*;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * @author Slight
 * <p>
 * Feb 26, 2022 8:45 PM
 */
public class DefaultApplierRedisCommandTest {

    private RedisOpParserManager parserManager = new DefaultRedisOpParserManager();

    private RedisOpSetParser parser = new RedisOpSetParser(parserManager);


    private static AsyncRedisClient client;

    @BeforeClass
    public static void beforeClass() throws Throwable {

        client = AsyncRedisClientFactory.DEFAULT.getOrCreateClient("DefaultApplierRedisCommandTest");
    }

    @AfterClass
    public static void afterClass() throws Exception {
        //destroy async redis client
    }

    private final ApplierSequenceController controller = new DefaultSequenceController();

    @Before
    public void setUp() throws Exception {
        controller.initialize();
    }

    @After
    public void tearDown() throws Exception {
        controller.dispose();
    }

    private RedisOp newSetOp(String... rawArgs) {
        return parser.parse(Lists.newArrayList(rawArgs));
    }

    @Test
    public void simple() throws Throwable {
        DefaultApplierRedisCommand command = new DefaultApplierRedisCommand(client, newSetOp("SET", "K", "V10"));
        command.execute().get();
    }

    @Test
    public void cooperateWithSequenceController() throws InterruptedException {
        DefaultApplierRedisCommand c1 = new DefaultApplierRedisCommand(client, newSetOp("SET", "K", "V10"));
        DefaultApplierRedisCommand c2 = new DefaultApplierRedisCommand(client, newSetOp("SET", "K", "V12"));
        DefaultApplierRedisCommand c3 = new DefaultApplierRedisCommand(client, newSetOp("SET", "K", "V14"));
        DefaultApplierRedisCommand c4 = new DefaultApplierRedisCommand(client, newSetOp("SET", "K", "V16"));

        controller.submit(c1);
        controller.submit(c2);
        controller.submit(c3);
        controller.submit(c4);
    }
}