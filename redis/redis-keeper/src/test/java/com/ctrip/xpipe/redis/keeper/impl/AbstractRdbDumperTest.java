package com.ctrip.xpipe.redis.keeper.impl;

import com.ctrip.xpipe.redis.core.store.DumpedRdbStore;
import com.ctrip.xpipe.redis.core.store.RdbDumpState;
import com.ctrip.xpipe.redis.keeper.AbstractFakeRedisTest;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisSlave;
import io.netty.channel.embedded.EmbeddedChannel;
import org.assertj.core.util.Sets;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

/**
 * @author Slight
 * <p>
 * Jun 08, 2021 5:29 PM
 */
public class AbstractRdbDumperTest extends AbstractFakeRedisTest {

    private RedisKeeperServer keeperServer;

    @Before
    public void beforeAbstractRdbDumperTest() throws Exception {
        keeperServer = startRedisKeeperServer(100, allCommandsSize, 1000);
    }

    @Test
    public void fixWaitDumpingConcurrencyProblem() throws Exception {
        //so many logs that pipeline does not pass, so loop for 10 times
        //you can increase the loop time to make sure the problem appear
        for (int i = 0; i < 10; i++) {
            RedisKeeperServer server = spy(keeperServer);

            RedisSlave redisSlave1 = fakeRedisSlave(server);
            RedisSlave redisSlave2 = fakeRedisSlave(server);
            doReturn(Sets.newLinkedHashSet(redisSlave1, redisSlave2)).when(server).slaves();

            AbstractRdbDumper dumper = spy(new TestDumper(server));
            doNothing().when(dumper).doFullSyncOrGiveUp(any(RedisSlave.class));
            server.setRdbDumper(dumper, true);

            CountDownLatch latch = new CountDownLatch(2);
            runTogether(() -> {
                redisSlave1.waitForRdbDumping();
                dumper.setRdbDumpState(RdbDumpState.DUMPING);
            }, latch);
            runTogether(() -> {
                try {
                    dumper.tryFullSync(redisSlave2);
                } catch (IOException ignored) {
                }
            }, latch);

            waitConditionUntilTimeOut(() -> {
                try {
                    verify(dumper, times(2)).doFullSyncOrGiveUp(any(RedisSlave.class));
                    return true;
                } catch (Throwable t) {
                    return false;
                }
            }, 2000);
        }
    }

    @Test
    public void ableToUpdateStateWhenFullSync() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        RedisKeeperServer server = spy(keeperServer);
        AbstractRdbDumper dumper = spy(new TestDumper(server) {
            @Override
            void doFullSyncOrGiveUp(RedisSlave redisSlave) throws IOException {
                latch.countDown();
                sleep(1000);
            }
        });
        dumper.setRdbDumpState(RdbDumpState.DUMPING);

        executor.execute(()->{
            try {
                dumper.tryFullSync(fakeRedisSlave(server));
            } catch (IOException ignored) {}
        });
        latch.await();
        long start = System.currentTimeMillis();
        dumper.setRdbDumpState(RdbDumpState.NORMAL);
        assertTrue(System.currentTimeMillis() - start < 100);
    }

    private ExecutorService executor = Executors.newFixedThreadPool(2);

    private void runTogether(Runnable runnable, CountDownLatch wait) {
        executor.execute(()->{
            try {
                wait.countDown();
                wait.await();
                runnable.run();
            } catch (InterruptedException ignored) { }
        });
    }

    private RedisSlave fakeRedisSlave(RedisKeeperServer server) {
        return new DefaultRedisSlave(new DefaultRedisClient(new EmbeddedChannel(), server));
    }

    class TestDumper extends AbstractRdbDumper {

        public TestDumper(RedisKeeperServer redisKeeperServer) {
            super(redisKeeperServer);
        }

        @Override
        protected void doExecute() throws Throwable {

        }

        @Override
        public DumpedRdbStore prepareRdbStore() throws IOException {
            return null;
        }
    }
}