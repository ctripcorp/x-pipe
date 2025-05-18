package com.ctrip.xpipe.redis.keeper.impl.fakeredis;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.netty.NettyPoolUtil;
import com.ctrip.xpipe.redis.core.protocal.cmd.InMemoryGapAllowedSync;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
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

        InMemoryGapAllowedSync firstSlave = sendInmemoryGAsync("127.0.0.1", backupDcKeeperServer.getListeningPort());
        firstSlave.future().addListener(f -> {
            logger.info("[firstSlave] {}", f.isSuccess(), f.cause());
        });

        waitConditionUntilTimeOut(() -> allCommandsSize == firstSlave.getCommands().length);

        long activeKeeperFirstAvailableOffset, backupKeeperFirstAvailableOffset;

        ReplicationStore activeStore = activeDcKeeperServer.getReplicationStore();
        ReplicationStore backupStore = backupDcKeeperServer.getReplicationStore();

        Assert.assertEquals(1, backupDcKeeperServer.getKeeperMonitor().getKeeperStats().getFullSyncCount());

        fakeRedisServer.reGenerateRdb();
        backupDcKeeperServer.releaseRdb();
        waitConditionUntilTimeOut(() -> allCommandsSize * 2 == firstSlave.getCommands().length);
        activeDcKeeperServer.getReplicationStore().gc();
        backupDcKeeperServer.getReplicationStore().gc();

        activeKeeperFirstAvailableOffset = activeDcKeeperServer.getReplicationStore().getMetaStore().backlogOffsetToReplOffset(activeDcKeeperServer.getReplicationStore().backlogBeginOffset());
        backupKeeperFirstAvailableOffset = backupDcKeeperServer.getReplicationStore().getMetaStore().backlogOffsetToReplOffset(backupDcKeeperServer.getReplicationStore().backlogBeginOffset());

        Assert.assertTrue(activeKeeperFirstAvailableOffset < backupKeeperFirstAvailableOffset);
        Assert.assertNotNull(activeDcKeeperServer.getReplicationStore().getMetaStore().dupReplicationStoreMeta().getRdbFile());

        long activeKeeperRdbContinousOffset = activeDcKeeperServer.getReplicationStore().getMetaStore().backlogOffsetToReplOffset(activeDcKeeperServer.getReplicationStore().getMetaStore().dupReplicationStoreMeta().getRdbContiguousBacklogOffset());
        Assert.assertTrue(activeKeeperRdbContinousOffset < backupKeeperFirstAvailableOffset);

        InMemoryGapAllowedSync secondSlave = sendInmemoryGAsync(NettyPoolUtil.createNettyPool(
                new DefaultEndPoint("127.0.0.1", backupDcKeeperServer.getListeningPort())), "?", -1, null);
        waitConditionUntilTimeOut(() -> allCommandsSize == secondSlave.getCommands().length);
        Assert.assertEquals(2, backupDcKeeperServer.getKeeperMonitor().getKeeperStats().getFullSyncCount());
        Assert.assertFalse(firstSlave.future().isDone());
        Assert.assertFalse(secondSlave.future().isDone());
    }

}
