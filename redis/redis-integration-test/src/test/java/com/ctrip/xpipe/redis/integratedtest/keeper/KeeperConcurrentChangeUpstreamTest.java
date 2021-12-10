package com.ctrip.xpipe.redis.integratedtest.keeper;

import com.ctrip.xpipe.api.cluster.LeaderElectorManager;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.core.protocal.MASTER_STATE;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import com.ctrip.xpipe.redis.core.protocal.cmd.RoleCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.SlaveOfCommand;
import com.ctrip.xpipe.redis.core.protocal.pojo.Role;
import com.ctrip.xpipe.redis.core.protocal.pojo.SlaveRole;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreManager;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import com.ctrip.xpipe.redis.keeper.impl.DefaultRedisKeeperServer;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

/**
 * @author lishanglin
 * date 2021/12/1
 */
public class KeeperConcurrentChangeUpstreamTest extends AbstractKeeperIntegratedMultiDc {

    private RedisKeeperServer backupDcActiveKeeper;

    private ReplicationStoreManager storeManager;

    private ReplicationStoreManager spyStoreManager;

    @Override
    protected RedisKeeperServer startKeeper(KeeperMeta keeperMeta, KeeperConfig keeperConfig,
                                            LeaderElectorManager leaderElectorManager) throws Exception {
        // mock backup dc active keeper
        RedisKeeperServer redisKeeperServer = super.startKeeper(keeperMeta, keeperConfig, leaderElectorManager);
        ClusterMeta clusterMeta = keeperMeta.parent().parent();
        DcMeta dcMeta = clusterMeta.parent();
        if (!keeperMeta.isActive() || clusterMeta.getActiveDc().equals(dcMeta.getId())) {
            return redisKeeperServer;
        }

        this.backupDcActiveKeeper = redisKeeperServer;
        this.storeManager = ((DefaultRedisKeeperServer)redisKeeperServer).getReplicationStoreManager();
        this.spyStoreManager = spy(this.storeManager);
        ((DefaultRedisKeeperServer) redisKeeperServer).setReplicationStoreManager(spyStoreManager);
        return redisKeeperServer;
    }

    @Override
    protected KeeperConfig getKeeperConfig() {
        TestKeeperConfig keeperConfig = new TestKeeperConfig();
        keeperConfig.setReplicationStoreCommandFileSize(10240);
        keeperConfig.setReplicationStoreCommandFileNumToKeep(Integer.MAX_VALUE);
        keeperConfig.setReplicationStoreMaxCommandsToTransferBeforeCreateRdb(Integer.MAX_VALUE);
        keeperConfig.setReplicationStoreGcIntervalSeconds(1000000);
        return keeperConfig;
    }

    @Test
    public void testUpstreamChangeOnReplIdChange_noFsync() throws Exception {
        sendMessageToMasterAndTestSlaveRedis(128);
        CyclicBarrier barrier = new CyclicBarrier(2);
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean hasHangReplChange = new AtomicBoolean(false);

        doAnswer(inv -> {
            ReplicationStore store = storeManager.createIfNotExist();
            ReplicationStore spyStore = spy(store);
            doAnswer(innerInv -> {
                String replId = innerInv.getArgumentAt(0, String.class);
                if (hasHangReplChange.compareAndSet(false, true)) {
                    logger.info("[testUpstreamChangeOnReplIdChange_noFsync] spy shiftReplicationId method");
                    store.shiftReplicationId(replId);
                    barrier.await();
                    latch.await(); // // wait for master change
                    sleep(1000); // hang for a moment, wait keeperServer dealing with master change
                } else {
                    store.shiftReplicationId(replId);
                }
                return null;
            }).when(spyStore).shiftReplicationId(anyString());
            return spyStore;
        }).when(spyStoreManager).createIfNotExist();

        long fullSyncCount = backupDcActiveKeeper.getKeeperMonitor().getKeeperStats().getFullSyncCount();

        String activeDc = getPrimaryDc();
        String backupDc = getBackupDc();
        stopKeeper(getRedisKeeperServer(getKeepersBackup(activeDc).get(0)));
        stopKeeper(getRedisKeeperServer(getKeepersBackup(backupDc).get(0)));

        RedisMeta master = getRedisMaster();
        RedisMeta promotedSlave = getRedisSlaves(activeDc).get(0);
        logger.info("[testUpstreamChangeOnReplIdChange_noFsync] master switch {} -> {}", master, promotedSlave);

        stopServerListeningPort(master.getPort());
        SlaveOfCommand slaveOfCommand = new SlaveOfCommand(getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint(promotedSlave.getIp(), promotedSlave.getPort())), scheduled);
        slaveOfCommand.execute().get(1, TimeUnit.SECONDS);

        KeeperMeta activeDcKeeperMeta = getKeeperActive(activeDc);
        KeeperMeta backupDcKeeperMeta = getKeeperActive(backupDc);

        setKeeperState(activeDcKeeperMeta, KeeperState.ACTIVE, promotedSlave.getIp(), promotedSlave.getPort());
        sendMessageToMaster(promotedSlave, 128);

        barrier.await(3, TimeUnit.SECONDS); // wait backup dc keeper reconnect to active dc keeper and replId change
        setKeeperState(backupDcKeeperMeta, KeeperState.ACTIVE, promotedSlave.getIp(), promotedSlave.getPort(), false); // backupDcActiveKeeper connect to Master

        latch.countDown();
        sendMesssageToMasterAndTest(128, promotedSlave, getRedisSlaves(backupDc));
        Assert.assertEquals(fullSyncCount, backupDcActiveKeeper.getKeeperMonitor().getKeeperStats().getFullSyncCount());
    }

}
