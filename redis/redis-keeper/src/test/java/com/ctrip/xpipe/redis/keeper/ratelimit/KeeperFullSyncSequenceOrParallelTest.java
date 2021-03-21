package com.ctrip.xpipe.redis.keeper.ratelimit;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.AbstractFakeRedisTest;
import org.junit.Test;

/**
 * @author zhuchen
 * Mar 2020/3/26 2020
 */
public class KeeperFullSyncSequenceOrParallelTest extends AbstractFakeRedisTest {

    // assume active dc keeper is connect to master first
    @Test
    public void testSequencialOrParallel() throws Exception {
        RedisKeeperServer primarySiteActiveKeeper = startRedisKeeperServer();
        RedisKeeperServer backupSiteActiveKeeper = startRedisKeeperServer();
        RedisKeeperServer backupSiteSlave = startRedisKeeperServer();
        connectToFakeRedis(primarySiteActiveKeeper);
        backupSiteActiveKeeper.getRedisKeeperServerState()
                .becomeActive(new DefaultEndPoint("127.0.0.1", primarySiteActiveKeeper.getListeningPort()));
        backupSiteSlave.getRedisKeeperServerState().becomeBackup(new DefaultEndPoint("127.0.0.1", backupSiteActiveKeeper.getListeningPort()));
        sleep(10000);
    }

    // assume backup dc keeper is connect to keeper first
    @Test
    public void testSequencialOrParallel2() throws Exception {
        RedisKeeperServer primarySiteActiveKeeper = startRedisKeeperServer();
        RedisKeeperServer backupSiteActiveKeeper = startRedisKeeperServer();
        RedisKeeperServer backupSiteSlave = startRedisKeeperServer();
        connectToFakeRedis(primarySiteActiveKeeper);
        sleep(5000);
        primarySiteActiveKeeper.getReplicationStore().close();
        backupSiteActiveKeeper.getRedisKeeperServerState()
                .becomeActive(new DefaultEndPoint("127.0.0.1", primarySiteActiveKeeper.getListeningPort()));
        backupSiteSlave.getRedisKeeperServerState().becomeBackup(new DefaultEndPoint("127.0.0.1", backupSiteActiveKeeper.getListeningPort()));

//        waitConditionUntilTimeOut(()->backupSiteSlave.getKeeperMonitor().getKeeperStats().getFullSyncCount() >= 1, 5000);
        sleep(10000);
    }
}
