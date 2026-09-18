package com.ctrip.xpipe.redis.integratedtest.keeper;

import com.ctrip.xpipe.api.cluster.LeaderElectorManager;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.netty.NettyPoolUtil;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import com.ctrip.xpipe.redis.core.protocal.cmd.SlaveOfCommand;
import com.ctrip.xpipe.redis.core.store.CommandsListener;
import com.ctrip.xpipe.redis.core.store.ReplicationProgress;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import com.ctrip.xpipe.redis.keeper.monitor.KeepersMonitorManager;
import com.ctrip.xpipe.redis.keeper.ratelimit.SyncRateManager;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStore;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

import static org.mockito.Mockito.*;

/**
 * @author lishanglin
 * date 2021/7/20
 */
public class KeeperCmdFileMissTest extends AbstractKeeperIntegratedSingleDc {

    private RedisKeeperServer spyActiveKeeperServer;

    @Override
    protected KeeperConfig getKeeperConfig() {
        TestKeeperConfig keeperConfig = new TestKeeperConfig();
        keeperConfig.setReplicationStoreCommandFileSize(256);
        keeperConfig.setReplicationStoreMaxCommandsToTransferBeforeCreateRdb(Integer.MAX_VALUE);
        keeperConfig.setReplicationStoreGcIntervalSeconds(1000000);
        return keeperConfig;
    }

    /**
     * Return a Mockito spy for the active keeper and let {@code startKeeper → add()} own
     * initialize/start/stop. Do <b>not</b> start the pre-spy instance: mockito-core subclass
     * spies are a different object that shallow-copies {@code lifecycleState}; starting the
     * original binds the listen port while the registry only stops the spy → port leak /
     * {@code BindException} in later AllKeeperTest cases.
     */
    @Override
    protected RedisKeeperServer createRedisKeeperServer(KeeperMeta keeperMeta, File baseDir, KeeperConfig keeperConfig,
                                                        LeaderElectorManager leaderElectorManager,
                                                        KeepersMonitorManager keeperMonitorManager, SyncRateManager syncRateManager) {
        RedisKeeperServer keeperServer = super.createRedisKeeperServer(keeperMeta, baseDir, keeperConfig, leaderElectorManager, keeperMonitorManager, syncRateManager);
        if (keeperMeta.equals(getKeeperActive())) {
            spyActiveKeeperServer = spy(keeperServer);
            return spyActiveKeeperServer;
        }

        return keeperServer;
    }

    @Test
    public void cmdFileMissingOnSendingCmdAfterFsync() throws Exception {
        sendMessageToMasterAndTestSlaveRedis(512);
        RedisMeta slave = getRedisSlaves().iterator().next();

        DefaultReplicationStore realStore = (DefaultReplicationStore) spyActiveKeeperServer.getReplicationStore();
        int originRdbDumpCnt = realStore.getRdbUpdateCount();

        doAnswer(serverParams -> {
            ReplicationStore replicationStore = (ReplicationStore) serverParams.callRealMethod();
            ReplicationStore spyReplicationStore = spy(replicationStore);
            doAnswer(storeParams -> {
                replicationStore.gc();
                logger.info("[cmdFileMissingOnSendingCmdAfterFsync] after gc");
                replicationStore.addCommandsListener(storeParams.getArgument(0, ReplicationProgress.class), storeParams.getArgument(1, CommandsListener.class));
                return null;
            }).when(spyReplicationStore).addCommandsListener(any(), any());

            return spyReplicationStore;
        }).when(spyActiveKeeperServer).getReplicationStore();

        SimpleObjectPool<NettyClient> slaveClientPool = NettyPoolUtil.createNettyPoolWithGlobalResource(new DefaultEndPoint(slave.getIp(), slave.getPort()));
        new SlaveOfCommand(slaveClientPool, scheduled).execute().get();
        new SlaveOfCommand(slaveClientPool, activeKeeper.getIp(), activeKeeper.getPort(), scheduled).execute().get();

        waitConditionUntilTimeOut(() -> {
            try {
                String info = new InfoCommand(slaveClientPool, InfoCommand.INFO_TYPE.REPLICATION, scheduled).execute().get();
                InfoResultExtractor extractor = new InfoResultExtractor(info);
                boolean masterLinkUp = extractor.extract("master_link_status").equalsIgnoreCase("up");
                long offset = Long.parseLong(extractor.extract("master_repl_offset"));
                logger.info("[cmdFileMissingOnSendingCmdAfterFsync] offset {}", offset);
                return masterLinkUp && offset > 1000;
            } catch (Exception e) {
                return false;
            }
        }, 30000, 2000);

        Assert.assertEquals(originRdbDumpCnt, realStore.getRdbUpdateCount());
    }

}
