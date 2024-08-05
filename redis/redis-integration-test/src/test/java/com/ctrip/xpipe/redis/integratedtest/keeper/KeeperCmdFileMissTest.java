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

    private RedisKeeperServer originActiveKeeperServer;

    private RedisKeeperServer spyActiveKeeperServer;

    @Override
    protected KeeperConfig getKeeperConfig() {
        TestKeeperConfig keeperConfig = new TestKeeperConfig();
        keeperConfig.setReplicationStoreCommandFileSize(256);
        keeperConfig.setReplicationStoreMaxCommandsToTransferBeforeCreateRdb(Integer.MAX_VALUE);
        keeperConfig.setReplicationStoreGcIntervalSeconds(1000000);
        return keeperConfig;
    }

    @Override
    protected RedisKeeperServer createRedisKeeperServer(KeeperMeta keeperMeta, File baseDir, KeeperConfig keeperConfig,
                                                        LeaderElectorManager leaderElectorManager,
                                                        KeepersMonitorManager keeperMonitorManager, SyncRateManager syncRateManager) {
        RedisKeeperServer keeperServer = super.createRedisKeeperServer(keeperMeta, baseDir, keeperConfig, leaderElectorManager, keeperMonitorManager, syncRateManager);
        if (keeperMeta.equals(getKeeperActive())) {
            originActiveKeeperServer = keeperServer;
            spyActiveKeeperServer = spy(keeperServer);
            try {
                originActiveKeeperServer.initialize();
                originActiveKeeperServer.start();
            } catch (Exception e) {
                logger.info("[createRedisKeeperServer] init fail", e);
            }

            return spyActiveKeeperServer;
        }

        return keeperServer;
    }

    @Test
    public void cmdFileMissingOnSendingCmdAfterFsync() throws Exception {
        sendMessageToMasterAndTestSlaveRedis(512);
        RedisMeta slave = getRedisSlaves().iterator().next();

        doAnswer(serverParams -> {
            ReplicationStore replicationStore = originActiveKeeperServer.getReplicationStore();
            ReplicationStore spyReplicationStore = spy(replicationStore);
            doAnswer(storeParams -> {
                replicationStore.gc();
                logger.info("[cmdFileMissingOnSendingCmdAfterFsync] after gc");
                replicationStore.addCommandsListener(storeParams.getArgument(0, ReplicationProgress.class), storeParams.getArgument(1, CommandsListener.class));
                return null;
            }).when(spyReplicationStore).addCommandsListener(any(), any());

            return spyReplicationStore;
        }).when(spyActiveKeeperServer).getReplicationStore();

        int originRdbDumpCnt = ((DefaultReplicationStore)originActiveKeeperServer.getReplicationStore()).getRdbUpdateCount();

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

        int currentRdbDumpCnt = ((DefaultReplicationStore)originActiveKeeperServer.getReplicationStore()).getRdbUpdateCount();
        Assert.assertEquals(originRdbDumpCnt, currentRdbDumpCnt);
    }

}
