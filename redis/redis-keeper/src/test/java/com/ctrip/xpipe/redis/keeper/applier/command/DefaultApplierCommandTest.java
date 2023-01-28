package com.ctrip.xpipe.redis.keeper.applier.command;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.client.redis.AsyncRedisClient;
import com.ctrip.xpipe.client.redis.AsyncRedisClientFactory;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParserManager;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.redis.core.redis.operation.parser.DefaultRedisOpParserManager;
import com.ctrip.xpipe.redis.core.redis.operation.parser.RedisOpSingleKeyParser;
import com.ctrip.xpipe.redis.keeper.applier.sequence.DefaultSequenceController;
import com.ctrip.xpipe.utils.ClusterShardAwareThreadFactory;
import org.junit.*;

import java.util.concurrent.Executors;

/**
 * @author Slight
 * <p>
 * Feb 26, 2022 8:45 PM
 */
public class DefaultApplierCommandTest extends AbstractTest {
    //manual test with no assert

    private RedisOpParserManager parserManager = new DefaultRedisOpParserManager();

    private RedisOpSingleKeyParser parser = new RedisOpSingleKeyParser(RedisOpType.SET, 1, 2);

    private static AsyncRedisClient client;

    @BeforeClass
    public static void beforeClass() throws Throwable {

        client = AsyncRedisClientFactory.DEFAULT.getOrCreateClient("DefaultApplierRedisCommandTest");
    }

    @AfterClass
    public static void afterClass() throws Exception {
        //destroy async redis client
    }

    private final DefaultSequenceController controller = new DefaultSequenceController();

    @Before
    public void setUp() throws Exception {
        controller.stateThread = Executors.newFixedThreadPool(1);
        controller.workerThreads = Executors.newScheduledThreadPool(1,
                ClusterShardAwareThreadFactory.create("test-cluster", "test-shard", "state-test-thread"));
        controller.scheduled = scheduled;
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
        RedisOpDataCommand command = new DefaultDataCommand(client, newSetOp("SET", "K", "V10"));
        command.execute().get();
    }

    @Test
    public void cooperateWithSequenceController() throws InterruptedException {
        RedisOpDataCommand c1 = new DefaultDataCommand(client, newSetOp("SET", "K", "V10"));
        RedisOpDataCommand c2 = new DefaultDataCommand(client, newSetOp("SET", "K", "V12"));
        RedisOpDataCommand c3 = new DefaultDataCommand(client, newSetOp("SET", "K", "V14"));
        RedisOpDataCommand c4 = new DefaultDataCommand(client, newSetOp("SET", "K", "V16"));

        controller.submit(c1);
        controller.submit(c2);
        controller.submit(c3);
        controller.submit(c4);
    }
}