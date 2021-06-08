package com.ctrip.xpipe.redis.keeper.impl;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
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

    private RedisSlave fakeRedisSlave(RedisKeeperServer server) {
        return new DefaultRedisSlave(new DefaultRedisClient(new EmbeddedChannel(), server));
    }

    @Test
    public void fixWaitDumpingConcurrencyProblem() throws Exception {

        for (int i = 0; i < 1000; i++) {

            RedisKeeperServer server = spy(keeperServer);

            RedisSlave redisSlave1 = fakeRedisSlave(server);
            RedisSlave redisSlave2 = fakeRedisSlave(server);
            doReturn(Sets.newLinkedHashSet(redisSlave1, redisSlave2)).when(server).slaves();

            AbstractRdbDumper dumper = spy(new AbstractRdbDumper(server) {

                @Override
                public DumpedRdbStore prepareRdbStore() throws IOException {
                    return null;
                }

                @Override
                protected void doExecute() throws Throwable {

                }
            });
            doNothing().when(dumper).doFullSyncOrGiveUp(any(RedisSlave.class));
            server.setRdbDumper(dumper, true);

            CountDownLatch wait = new CountDownLatch(2);
            CountDownLatch done = new CountDownLatch(2);

            runTogether(() -> {
                redisSlave1.waitForRdbDumping();
                dumper.setRdbDumpState(RdbDumpState.DUMPING);
            }, wait, done);

            runTogether(() -> {
                try {
                    dumper.tryFullSync(redisSlave2);
                } catch (IOException ignored) {
                }
            }, wait, done);

            waitConditionUntilTimeOut(()->{
                try {
                    verify(dumper, times(2)).doFullSyncOrGiveUp(any(RedisSlave.class));
                    return true;
                } catch (Throwable t) {
                    return false;
                }
            });
        }
    }

    private ExecutorService server = Executors.newFixedThreadPool(2);

    private void runTogether(Runnable runnable, CountDownLatch wait, CountDownLatch done) {
        server.execute(()->{
            try {
                wait.countDown();
                wait.await();
                runnable.run();
                done.countDown();
            } catch (InterruptedException ignored) { }
        });
    }
}