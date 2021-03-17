package com.ctrip.xpipe.redis.keeper.ratelimit;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.core.protocal.MASTER_STATE;
import com.ctrip.xpipe.redis.keeper.AbstractFakeRedisTest;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.KeeperResourceManager;
import com.ctrip.xpipe.utils.LeakyBucket;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.concurrent.TimeoutException;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class RateLimitTest extends AbstractFakeRedisTest {

    @Mock
    private KeeperResourceManager resourceManager;
    @Mock
    private LeakyBucket leakyBucket;


    private RedisKeeperServer redisKeeperServer1;
    private RedisKeeperServer redisKeeperServer2;


    @Before
    public void beforeRateLimitTest() throws Exception {

        redisKeeperServer1 = startRedisKeeperServer();
        redisKeeperServer2 = startRedisKeeperServer();
        logger.info(remarkableMessage("keeper1 {}, keeper2 {}"), redisKeeperServer1.getListeningPort(), redisKeeperServer2.getListeningPort());


        when(resourceManager.getLeakyBucket()).thenReturn(leakyBucket);
        when(leakyBucket.tryAcquire()).thenReturn(true);
    }


    @Override
    public KeeperResourceManager getResourceManager() {
        return resourceManager;
    }

    @Test
    public void testBackupDcActiveKeeper_DrMigration_ShouldLimit() throws Exception {

        Assert.assertEquals(0, fakeRedisServer.getConnected());
        int round = 1;

        for (int i = 0; i < round; i++) {

            int finalI = i;

            redisKeeperServer1.getRedisKeeperServerState().becomeActive(new DefaultEndPoint("localhost", fakeRedisServer.getPort()));
            waitConditionUntilTimeOut(() -> { return fakeRedisServer.getPsyncCount() >= (finalI +1); }, 1000);

            verify(leakyBucket, times(i)).tryAcquire();

            logger.info(remarkableMessage("start keeper2 and make keeper1 connect to it keeper1:{}, keeper2:{}"),
                    redisKeeperServer1.getListeningPort(), redisKeeperServer2.getListeningPort());
            redisKeeperServer1.getRedisKeeperServerState().becomeActive(new DefaultEndPoint("127.0.0.1", redisKeeperServer2.getListeningPort()));

            sleep(300);
            verify(leakyBucket, times(i+1)).tryAcquire();
        }
    }


    @Test
    public void testBackupDcActiveKeeper_DownForALongTime_ShouldLimit() throws Exception {

        redisKeeperServer2.getRedisKeeperServerState().becomeActive(new DefaultEndPoint("localhost", fakeRedisServer.getPort()));
        waitConditionUntilTimeOut(() -> { return fakeRedisServer.getPsyncCount() >= (1); }, 1000);

        redisKeeperServer1.getRedisKeeperServerState().becomeActive(new DefaultEndPoint("127.0.0.1", redisKeeperServer2.getListeningPort()));
        waitConditionUntilTimeOut(()->(redisKeeperServer2.slaves().size() >=1));
        sleep(200);
        verify(leakyBucket, times(1)).tryAcquire();

        logger.info(remarkableMessage("stop keeper2 {}"), redisKeeperServer2.getListeningPort());
        redisKeeperServer2.stop();
        sleep((int) (redisKeeperServer1.getKeeperConfig().getReplDownSafeIntervalMilli()*3/2));

        logger.info(remarkableMessage("start keeper2 {}"), redisKeeperServer2.getListeningPort());
        redisKeeperServer2.start();
        waitConditionUntilTimeOut(()->(redisKeeperServer2.slaves().size() >=1));

        verify(leakyBucket, times(2)).tryAcquire();

    }

    @Test
    public void testBackupDcActiveKeeper_RestartAsActive_ShouldNotLimit() throws TimeoutException {

        redisKeeperServer2.getRedisKeeperServerState().becomeActive(new DefaultEndPoint("localhost", fakeRedisServer.getPort()));
        waitConditionUntilTimeOut(() -> { return fakeRedisServer.getPsyncCount() >= (1); });

        redisKeeperServer1.getRedisKeeperServerState().becomeActive(new DefaultEndPoint("127.0.0.1", redisKeeperServer2.getListeningPort()));
        sleep(300);
        verify(leakyBucket, times(0)).tryAcquire();

    }


    @Test
    public void testBackupDcActiveKeeper_BackupToActive_ShouldNotLimit() throws Exception {


        RedisKeeperServer redisKeeperServer3 = startRedisKeeperServer();
        redisKeeperServer1.getRedisKeeperServerState().becomeActive(new DefaultEndPoint("127.0.0.1", fakeRedisServer.getPort()));
        waitConditionUntilTimeOut(() -> { return redisKeeperServer1.getRedisMaster().getMasterState() == MASTER_STATE.REDIS_REPL_CONNECTED; });
        redisKeeperServer2.getRedisKeeperServerState().becomeActive(new DefaultEndPoint("localhost", redisKeeperServer1.getListeningPort()));
        waitConditionUntilTimeOut(() -> { return redisKeeperServer2.getRedisMaster().getMasterState() == MASTER_STATE.REDIS_REPL_CONNECTED; });
        redisKeeperServer3.getRedisKeeperServerState().becomeBackup(new DefaultEndPoint("localhost", redisKeeperServer2.getListeningPort()));
        waitConditionUntilTimeOut(() -> { return redisKeeperServer3.getRedisMaster().getMasterState() == MASTER_STATE.REDIS_REPL_CONNECTED; });

        verify(leakyBucket, times(1)).tryAcquire();

        logger.info(remarkableMessage("make keeper3 becom active"));
        redisKeeperServer3.getRedisKeeperServerState().becomeActive(new DefaultEndPoint("127.0.0.1", redisKeeperServer1.getListeningPort()));
        waitConditionUntilTimeOut(() -> { return redisKeeperServer1.slaves().size() >= 2; });
        verify(leakyBucket, times(1)).tryAcquire();

    }

}
