package com.ctrip.xpipe.redis.keeper.ratelimit;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.core.protocal.MASTER_STATE;
import com.ctrip.xpipe.redis.keeper.AbstractFakeRedisTest;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.KeeperResourceManager;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import com.ctrip.xpipe.redis.keeper.spring.TestWithoutZkProfile;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStore;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.ctrip.xpipe.utils.LeakyBucket;
import com.ctrip.xpipe.utils.OsUtils;
import org.junit.*;
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

    private int replDownSafeIntervalMilli = 1200;
    private int BEFORE_APPROXIMATE__RESTART_TIME_MILLI;


    @Before
    public void beforeRateLimitTest() throws Exception {

        BEFORE_APPROXIMATE__RESTART_TIME_MILLI = OsUtils.APPROXIMATE__RESTART_TIME_MILLI;
        OsUtils.APPROXIMATE__RESTART_TIME_MILLI = replDownSafeIntervalMilli / 100;
        redisKeeperServer1 = startRedisKeeperServer();
        redisKeeperServer2 = startRedisKeeperServer();
        logger.info(remarkableMessage("keeper1 {}, keeper2 {}"), redisKeeperServer1.getListeningPort(), redisKeeperServer2.getListeningPort());

        when(resourceManager.getLeakyBucket()).thenReturn(leakyBucket);
        when(leakyBucket.tryAcquire()).thenReturn(true);
        when(leakyBucket.getTotalSize()).thenReturn(redisKeeperServer1.getKeeperConfig().getLeakyBucketInitSize());

    }

    @Override
    protected KeeperConfig newTestKeeperConfig(int commandFileSize, int replicationStoreCommandFileNumToKeep, int replicationStoreMaxCommandsToTransferBeforeCreateRdb, int minTimeMilliToGcAfterCreate) {

        KeeperConfig config = super.newTestKeeperConfig(commandFileSize, replicationStoreCommandFileNumToKeep, replicationStoreMaxCommandsToTransferBeforeCreateRdb, minTimeMilliToGcAfterCreate);
        ((TestKeeperConfig) config).setReplDownSafeIntervalMilli(replDownSafeIntervalMilli);
        return config;
    }

    @Override
    public KeeperResourceManager getResourceManager() {
        return resourceManager;
    }

    @Test
    public void testBackupDcActiveKeeper_DrMigration_ShouldLimit() throws Exception {

        Assert.assertEquals(0, fakeRedisServer.getConnected());
        int round = 10;

        redisKeeperServer2.getRedisKeeperServerState().becomeActive(new DefaultEndPoint("localhost", fakeRedisServer.getPort()));
        waitRedisKeeperServerConnected(redisKeeperServer2);

        redisKeeperServer1.getRedisKeeperServerState().becomeActive(new DefaultEndPoint("localhost", fakeRedisServer.getPort()));
        waitRedisKeeperServerConnected(redisKeeperServer1);

        verify(leakyBucket, never()).tryAcquire();

        logger.info(remarkableMessage("start keeper2 and make keeper1 connect to it keeper1:{}, keeper2:{}"),
                redisKeeperServer1.getListeningPort(), redisKeeperServer2.getListeningPort());
        redisKeeperServer1.getRedisKeeperServerState().becomeActive(new DefaultEndPoint("127.0.0.1", redisKeeperServer2.getListeningPort()));

        waitRedisKeeperServerConnected(redisKeeperServer1);
        waitConditionUntilTimeOut(()->assertSuccess(()->{
            verify(leakyBucket, times(1)).tryAcquire();
            verify(leakyBucket, times(1)).release();
        }));
    }


    @Test
    public void testBackupDcActiveKeeper_DownForALongTime_ShouldLimit() throws Exception {

        redisKeeperServer2.getRedisKeeperServerState().becomeActive(new DefaultEndPoint("localhost", fakeRedisServer.getPort()));

        redisKeeperServer1.getRedisKeeperServerState().becomeActive(new DefaultEndPoint("127.0.0.1", redisKeeperServer2.getListeningPort()));
        waitRedisKeeperServerConnected(redisKeeperServer2);
        waitRedisKeeperServerConnected(redisKeeperServer1);
        waitConditionUntilTimeOut(()->assertSuccess(()->{
            verify(leakyBucket, times(0)).tryAcquire();
        }));
        logger.info(remarkableMessage("stop keeper2 {}"), redisKeeperServer2.getListeningPort());
        redisKeeperServer2.stop();
        sleep((int) (redisKeeperServer1.getKeeperConfig().getReplDownSafeIntervalMilli() * 3 / 2));

        logger.info(remarkableMessage("start keeper2 {}"), redisKeeperServer2.getListeningPort());
        redisKeeperServer2.start();
        waitRedisKeeperServerConnected(redisKeeperServer1);
        waitConditionUntilTimeOut(()->assertSuccess(()->{
            verify(leakyBucket, times(1)).tryAcquire();
            verify(leakyBucket, times(1)).release();
        }));
    }

    @Test
    public void testBackupDcActiveKeeper_RdbDump_ShouldLimit() throws Exception {

        fakeRedisServer.setIsKeeper(true);
        RedisKeeperServer redisKeeperServer = startRedisKeeperServerAndConnectToFakeRedis(100, (int) (allCommandsSize * 0.8));
        logger.info("currentkeeper:{}", redisKeeperServer.getListeningPort());

        waitRedisKeeperServerConnected(redisKeeperServer);
        logger.info(remarkableMessage("begin wait for commands"));
        sleep(1000);//wait for full commands

        int rdbDumpCount1 = ((DefaultReplicationStore)redisKeeperServer.getReplicationStore()).getRdbUpdateCount();
        sendInmemoryPsync("127.0.0.1", redisKeeperServer.getListeningPort());
        waitConditionUntilTimeOut(() -> { return ((DefaultReplicationStore) redisKeeperServer.getReplicationStore()).getRdbUpdateCount() > rdbDumpCount1;  });
        logger.info(remarkableMessage("begin wait for rdb dump finished"));

        waitConditionUntilTimeOut(()->assertSuccess(()->{
            verify(leakyBucket, times(1)).tryAcquire();
            verify(leakyBucket, times(1)).release();
        }));
    }


    @Test
    public void testBackupDcActiveKeeper_RestartAsActive_ShouldNotLimit() throws TimeoutException {

        redisKeeperServer2.getRedisKeeperServerState().becomeActive(new DefaultEndPoint("localhost", fakeRedisServer.getPort()));
        waitConditionUntilTimeOut(() -> {
            return fakeRedisServer.getPsyncCount() >= (1);
        });

        redisKeeperServer1.getRedisKeeperServerState().becomeActive(new DefaultEndPoint("127.0.0.1", redisKeeperServer2.getListeningPort()));
        waitRedisKeeperServerConnected(redisKeeperServer1);
        verify(leakyBucket, times(0)).tryAcquire();

    }


    @Test
    public void testBackupDcActiveKeeper_BackupToActive_ShouldNotLimit() throws Exception {


        RedisKeeperServer redisKeeperServer3 = startRedisKeeperServer();
        redisKeeperServer1.getRedisKeeperServerState().becomeActive(new DefaultEndPoint("127.0.0.1", fakeRedisServer.getPort()));
        waitConditionUntilTimeOut(() -> {
            return redisKeeperServer1.getRedisMaster().getMasterState() == MASTER_STATE.REDIS_REPL_CONNECTED;
        });
        redisKeeperServer2.getRedisKeeperServerState().becomeActive(new DefaultEndPoint("localhost", redisKeeperServer1.getListeningPort()));
        waitConditionUntilTimeOut(() -> {
            return redisKeeperServer2.getRedisMaster().getMasterState() == MASTER_STATE.REDIS_REPL_CONNECTED;
        });
        redisKeeperServer3.getRedisKeeperServerState().becomeBackup(new DefaultEndPoint("localhost", redisKeeperServer2.getListeningPort()));
        waitConditionUntilTimeOut(() -> {
            return redisKeeperServer3.getRedisMaster().getMasterState() == MASTER_STATE.REDIS_REPL_CONNECTED;
        });

        verify(leakyBucket, times(0)).tryAcquire();

        logger.info(remarkableMessage("make keeper3 becom active"));
        redisKeeperServer3.getRedisKeeperServerState().becomeActive(new DefaultEndPoint("127.0.0.1", redisKeeperServer1.getListeningPort()));
        waitConditionUntilTimeOut(() -> {
            return redisKeeperServer1.slaves().size() >= 2;
        });
        verify(leakyBucket, times(0)).tryAcquire();
    }

    @Test
    public void testTokenWillBeFreedDueToFullSyncFailure() throws Exception {

        fakeRedisServer.setSendHalfRdbAndCloseConnectionCount(1);
        fakeRedisServer.setIsKeeper(true);

        sleep((int) (redisKeeperServer1.getKeeperConfig().getReplDownSafeIntervalMilli() * 3 / 2));
        redisKeeperServer1.getRedisKeeperServerState().becomeActive(new DefaultEndPoint("127.0.0.1", fakeRedisServer.getPort()));

        waitRedisKeeperServerConnected(redisKeeperServer1);
        sleep(200);
        waitConditionUntilTimeOut(()->assertSuccess(()->{
            verify(leakyBucket, times(2)).tryAcquire();
            verify(leakyBucket, times(2)).release();
        }));
    }

    @Test
    public void testTokenShallReleaseAfterPartialSyncFailure() throws Exception {

        fakeRedisServer.setIsKeeper(true);
        fakeRedisServer.setPartialSyncFail(true);

        redisKeeperServer1.getRedisKeeperServerState().becomeActive(new DefaultEndPoint("127.0.0.1", fakeRedisServer.getPort()));
        waitRedisKeeperServerConnected(redisKeeperServer1);
        verify(leakyBucket, never()).tryAcquire();

        redisKeeperServer1.getRedisKeeperServerState().becomeActive(new DefaultEndPoint("127.0.0.1", 0));
        sleep((int) (redisKeeperServer1.getKeeperConfig().getReplDownSafeIntervalMilli() * 3 / 2));
        logger.info(remarkableMessage("connect to fake server again, to mock partial sync fail"));
        redisKeeperServer1.getRedisKeeperServerState().becomeActive(new DefaultEndPoint("127.0.0.1", fakeRedisServer.getPort()));
        waitConditionUntilTimeOut(()->assertSuccess(()->{
            verify(leakyBucket, times(1)).tryAcquire();
            verify(leakyBucket, times(1)).release();
        }));
    }

    @After
    public void afterRateLimitTest(){
        OsUtils.APPROXIMATE__RESTART_TIME_MILLI = BEFORE_APPROXIMATE__RESTART_TIME_MILLI;
    }
}
