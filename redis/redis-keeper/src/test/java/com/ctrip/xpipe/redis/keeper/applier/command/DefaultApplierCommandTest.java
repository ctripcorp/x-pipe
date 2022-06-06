package com.ctrip.xpipe.redis.keeper.applier.command;

import com.ctrip.xpipe.client.redis.AsyncRedisClient;
import com.ctrip.xpipe.client.redis.AsyncRedisClientFactory;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParserManager;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.redis.core.redis.operation.parser.DefaultRedisOpParserManager;
import com.ctrip.xpipe.redis.core.redis.operation.parser.RedisOpSingleKVParser;
import com.ctrip.xpipe.redis.keeper.applier.sequence.ApplierSequenceController;
import com.ctrip.xpipe.redis.keeper.applier.sequence.DefaultSequenceController;
import org.assertj.core.util.Lists;
import org.junit.*;

/**
 * @author Slight
 * <p>
 * Feb 26, 2022 8:45 PM
 */
public class DefaultApplierCommandTest {

    private RedisOpParserManager parserManager = new DefaultRedisOpParserManager();

    private RedisOpSingleKVParser parser = new RedisOpSingleKVParser(RedisOpType.SET, 1, 2);

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
        return parser.parse(rawArgs);
    }

    @Test
    public void simple() throws Throwable {
        RedisOpCommand command = new DefaultApplierCommand(client, newSetOp("SET", "K", "V10"));
        command.execute().get();
    }

    @Test
    public void cooperateWithSequenceController() throws InterruptedException {
        RedisOpCommand c1 = new DefaultApplierCommand(client, newSetOp("SET", "K", "V10"));
        RedisOpCommand c2 = new DefaultApplierCommand(client, newSetOp("SET", "K", "V12"));
        RedisOpCommand c3 = new DefaultApplierCommand(client, newSetOp("SET", "K", "V14"));
        RedisOpCommand c4 = new DefaultApplierCommand(client, newSetOp("SET", "K", "V16"));

        controller.submit(c1);
        controller.submit(c2);
        controller.submit(c3);
        controller.submit(c4);
    }
}