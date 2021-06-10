package com.ctrip.xpipe.redis.keeper.impl;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.keeper.AbstractFakeRedisTest;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author Slight
 * <p>
 * Jun 07, 2021 6:24 PM
 */
public class StateBackupDeadlockTest extends AbstractFakeRedisTest {

    private RedisKeeperServer keeperServerA;
    private RedisKeeperServer keeperServerB;

    @Before
    public void beforeStateBackupDeadlockTest() throws Exception {

        keeperServerA = startRedisKeeperServer(100, allCommandsSize, 1000);
        keeperServerB = startRedisKeeperServer(100, allCommandsSize, 1000);

        keeperServerA.getRedisKeeperServerState().becomeActive(new DefaultEndPoint("localhost", fakeRedisServer.getPort()));

    }

    @Test
    public void test() throws Exception {
        //so many logs that pipeline does not pass, so loop for 10 times
        //you can increase the loop time to make sure the problem appear
        for (int i = 0; i < 10; i++) {
            keeperServerB.getRedisKeeperServerState().becomeBackup(new DefaultEndPoint("localhost", keeperServerA.getListeningPort()));
            keeperServerB.getRedisKeeperServerState().psync(new DefaultRedisClient(new EmbeddedChannel(), keeperServerB), null);
            CountDownLatch latch = new CountDownLatch(2);
            runTogether(() -> {
                keeperServerB.getRedisKeeperServerState().becomeActive(new DefaultEndPoint("localhost", fakeRedisServer.getPort()));
            }, latch);

            runTogether(() -> {
                try {
                    ((DefaultRedisKeeperServer) keeperServerB).replicationStoreManager.create();
                } catch (IOException ignored) {
                }
            }, latch);
        }
    }

    private ExecutorService executor = Executors.newFixedThreadPool(2);

    private void runTogether(Runnable runnable, CountDownLatch latch) {
        executor.execute(()->{
            try {
                latch.countDown();
                latch.await();
                runnable.run();
            } catch (InterruptedException ignored) { }
        });
    }
}