package com.ctrip.xpipe.redis.keeper.impl.fakeredis;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.keeper.AbstractFakeRedisTest;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStore;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * @author lishanglin
 * date 2021/8/13
 */
public class PsyncForKeeperTest extends AbstractFakeRedisTest {

    @Test
    public void testResetReplAfterLongTimeDown() throws Exception {
        RedisKeeperServer keeperServer = startRedisKeeperServerAndConnectToFakeRedis();
        waitRedisKeeperServerConnected(keeperServer);

        RedisKeeperServer newKeeperServer = restartKeeperServer(keeperServer, 1, 1);
        Assert.assertNull(newKeeperServer.getKeeperRepl().replId());
    }

    @Test
    public void testEmptyKeeperStartAndPartialSync() throws Exception {
        RedisKeeperServer keeperServer = startRedisKeeperServerAndConnectToFakeRedis(1, 100);
        waitCmdNotContinueWithRdb(keeperServer);

        DefaultReplicationStore replicationStore = (DefaultReplicationStore)keeperServer.getReplicationStore();
        int originRdbDumpCnt = replicationStore.getRdbUpdateCount();

        RedisKeeperServer keeperServer2 = startRedisKeeperServer();
        keeperServer2.getRedisKeeperServerState().becomeBackup(new DefaultEndPoint("localhost", keeperServer.getListeningPort()));
        waitRedisKeeperServerConnected(keeperServer2);

        Assert.assertEquals(originRdbDumpCnt, replicationStore.getRdbUpdateCount());
    }

    @Test
    public void testKeeperStartWithDataAndPartialSync() throws Exception {
        RedisKeeperServer keeperServer = startRedisKeeperServer(1, 1, 1000);
        RedisKeeperServer keeperServer2 = startRedisKeeperServer(1, 1, 1000);
        keeperServer.getRedisKeeperServerState().becomeActive(new DefaultEndPoint("localhost", fakeRedisServer.getPort()));
        waitRedisKeeperServerConnected(keeperServer);
        keeperServer2.getRedisKeeperServerState().becomeBackup(new DefaultEndPoint("localhost", keeperServer.getListeningPort()));
        waitRedisKeeperServerConnected(keeperServer2);

        waitCmdNotContinueWithRdb(keeperServer);
        waitCmdNotContinueWithRdb(keeperServer2);
        waitKeeperSyncWithRedis(keeperServer2);

        keeperServer2.stop();
        keeperServer2.dispose();

        fakeRedisServer.reGenerateRdb();
        waitKeeperSyncWithRedis(keeperServer);
        keeperServer.getReplicationStore().gc();
        DefaultReplicationStore replicationStore = (DefaultReplicationStore)keeperServer.getReplicationStore();
        int originRdbDumpCnt = replicationStore.getRdbUpdateCount();

        keeperServer2 = restartKeeperServer(keeperServer2, 1, 1);
        keeperServer2.getRedisKeeperServerState().becomeBackup(new DefaultEndPoint("localhost", keeperServer.getListeningPort()));
        waitRedisKeeperServerConnected(keeperServer2);

        Assert.assertEquals(originRdbDumpCnt, replicationStore.getRdbUpdateCount());
    }

    @Test
    public void testKeeperReFullSyncAfterRedisReplChange() throws Exception {
        RedisKeeperServer keeperServer = startRedisKeeperServerAndConnectToFakeRedis(1000, 10000);
        RedisKeeperServer keeperServer2 = startRedisKeeperServer(1, 10000, 100000);
        RedisKeeperServer keeperServer3 = startRedisKeeperServer(1, 10000, 100000);

        keeperServer2.getRedisKeeperServerState().becomeActive(new DefaultEndPoint("localhost", keeperServer.getListeningPort()));
        waitCmdNotContinueWithRdb(keeperServer2);
        waitKeeperSyncWithRedis(keeperServer2);
        DefaultReplicationStore replicationStore = (DefaultReplicationStore)keeperServer2.getReplicationStore();

        int port = randomPort();
        keeperServer3.getRedisKeeperServerState().becomeBackup(new DefaultEndPoint("localhost", port));
        keeperServer3.reconnectMaster();
        waitConditionUntilTimeOut(() -> keeperServer3.getTryConnectMasterCnt() > 1, 10000, 1000);
        // not first connect and send psync ? -1
        keeperServer3.getRedisKeeperServerState().becomeBackup(new DefaultEndPoint("localhost", keeperServer2.getListeningPort()));
        // keeperServer2 find rdb from keeperServer1 not continue then close store and send psync ? -1
        waitKeeperSyncWithRedis(keeperServer3);
        Assert.assertNotNull(keeperServer3.getReplicationStore().getMetaStore().dupReplicationStoreMeta().getRdbFile());
    }

    private RedisKeeperServer restartKeeperServer(RedisKeeperServer keeperServer, long replKeepSecondsAfterDown, long waitSecondsAfterDown) throws Exception {
        int keeperPort = keeperServer.getListeningPort();
        String keeperRunId = keeperServer.getKeeperRunid();

        LifecycleHelper.stopIfPossible(keeperServer);
        LifecycleHelper.disposeIfPossible(keeperServer);

        KeeperConfig keeperConfig = newTestKeeperConfig();
        ((TestKeeperConfig)keeperConfig).setReplKeepSecondsAfterDown(replKeepSecondsAfterDown);
        KeeperMeta keeperMeta = createKeeperMeta(keeperPort, keeperRunId);
        TimeUnit.SECONDS.sleep(waitSecondsAfterDown);

        return startRedisKeeperServer(keeperConfig, keeperMeta);
    }

    private void waitKeeperSyncWithRedis(RedisKeeperServer keeperServer) throws Exception {
        waitConditionUntilTimeOut(() -> keeperServer.getKeeperRepl().getEndOffset() == fakeRedisServer.getRdbOffset() + fakeRedisServer.getCommandsLength());
    }

    private void waitCmdNotContinueWithRdb(RedisKeeperServer keeperServer) throws Exception {
        waitConditionUntilTimeOut(() -> {
            if (null == keeperServer.getKeeperRepl().replId()) return false;
            try {
                keeperServer.getReplicationStore().gc();
            } catch (Exception e) {
                logger.info("[testEmptyKeeperStartAndPartialSync] gc fail", e);
            }

            long firstAvailableOffset = keeperServer.getReplicationStore().firstAvailableOffset();
            Long rdbOffset = keeperServer.getReplicationStore().getMetaStore().dupReplicationStoreMeta().getRdbLastOffset();
            return null == rdbOffset || firstAvailableOffset > rdbOffset + 1;
        }, 5000, 1000);
    }

}
