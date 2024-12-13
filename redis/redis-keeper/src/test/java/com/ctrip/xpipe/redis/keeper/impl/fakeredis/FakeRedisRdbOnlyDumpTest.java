package com.ctrip.xpipe.redis.keeper.impl.fakeredis;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.netty.NettyPoolUtil;
import com.ctrip.xpipe.redis.core.protocal.cmd.InMemoryPsync;
import com.ctrip.xpipe.redis.keeper.AbstractFakeRedisTest;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStore;
import org.junit.Assert;
import org.junit.Test;

public class FakeRedisRdbOnlyDumpTest extends AbstractFakeRedisTest {

    @Test
    public void testRdbOnlyNotClearStoreForNotContinue() throws Exception {
        RedisKeeperServer activeDcKeeperServer = startRedisKeeperServerAndConnectToFakeRedis();
        RedisKeeperServer backupDcKeeperServer = startRedisKeeperServer(1, allCommandsSize, 1);
        backupDcKeeperServer.getRedisKeeperServerState().becomeActive(new DefaultEndPoint("127.0.0.1", activeDcKeeperServer.getListeningPort()));
        ((DefaultReplicationStore)backupDcKeeperServer.getReplicationStore()).setCommandsRetainTimeoutMilli(1);
        waitRedisKeeperServerConnected(backupDcKeeperServer);

        InMemoryPsync firstSlave = sendInmemoryPsync("127.0.0.1", backupDcKeeperServer.getListeningPort());
        firstSlave.future().addListener(f -> {
            logger.info("[firstSlave] {}", f.isSuccess(), f.cause());
        });
        waitConditionUntilTimeOut(() -> allCommandsSize == firstSlave.getCommands().length);
        Assert.assertEquals(1, backupDcKeeperServer.getKeeperMonitor().getKeeperStats().getFullSyncCount());

        fakeRedisServer.reGenerateRdb();
        backupDcKeeperServer.releaseRdb();
        waitConditionUntilTimeOut(() -> allCommandsSize * 2 == firstSlave.getCommands().length);
        activeDcKeeperServer.getReplicationStore().gc();
        backupDcKeeperServer.getReplicationStore().gc();
        Assert.assertTrue(activeDcKeeperServer.getReplicationStore().firstAvailableOffset() < backupDcKeeperServer.getReplicationStore().firstAvailableOffset());
        Assert.assertNotNull(activeDcKeeperServer.getReplicationStore().getMetaStore().dupReplicationStoreMeta().getRdbFile());
        Assert.assertTrue(activeDcKeeperServer.getReplicationStore().getMetaStore().dupReplicationStoreMeta().getRdbLastOffset() + 1
                < backupDcKeeperServer.getReplicationStore().firstAvailableOffset());

        InMemoryPsync secondSlave = sendInmemoryPsync(NettyPoolUtil.createNettyPool(
                new DefaultEndPoint("127.0.0.1", backupDcKeeperServer.getListeningPort())), "?", -1, null);
        waitConditionUntilTimeOut(() -> allCommandsSize == secondSlave.getCommands().length);
        Assert.assertEquals(2, backupDcKeeperServer.getKeeperMonitor().getKeeperStats().getFullSyncCount());
        Assert.assertFalse(firstSlave.future().isDone());
        Assert.assertFalse(secondSlave.future().isDone());
    }

}
