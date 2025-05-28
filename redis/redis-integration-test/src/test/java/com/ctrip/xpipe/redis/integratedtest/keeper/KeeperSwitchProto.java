package com.ctrip.xpipe.redis.integratedtest.keeper;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.netty.NettyPoolUtil;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.ctrip.xpipe.redis.core.protocal.MASTER_STATE.REDIS_REPL_CONNECTED;

public class KeeperSwitchProto extends AbstractKeeperIntegratedMultiDcXsync {

    @Override
    protected KeeperConfig getKeeperConfig() {
        TestKeeperConfig keeperConfig = new TestKeeperConfig();
        keeperConfig.setReplicationStoreMaxCommandsToTransferBeforeCreateRdb(Integer.MAX_VALUE);
        keeperConfig.setReplicationStoreGcIntervalSeconds(1000);
        keeperConfig.setReplicationStoreCommandFileSize(1024);
        return keeperConfig;
    }

    @Test
    public void testSwitchOnWriting() throws Exception {
        setRedisToGtidEnabled(getRedisMaster().getIp(), getRedisMaster().getPort());
        for(RedisMeta slave : getRedisSlaves()) {
            setRedisToGtidEnabled(slave.getIp(), slave.getPort());
        }

        for(KeeperMeta keeperMeta : getDcKeepers(getPrimaryDc(), getClusterId(), getShardId())) {
            waitConditionUntilTimeOut(() -> getRedisKeeperServer(keeperMeta).getRedisMaster().getMasterState().equals(REDIS_REPL_CONNECTED));
        }

        for(KeeperMeta keeperMeta : getDcKeepers(getBackupDc(), getClusterId(), getShardId())) {
            waitConditionUntilTimeOut(() -> getRedisKeeperServer(keeperMeta).getRedisMaster().getMasterState().equals(REDIS_REPL_CONNECTED));
        }

        sendMessageToMaster(getRedisMaster(), 10);

        Thread.sleep(2000);

        RedisMeta master = getRedisMaster();
        SimpleObjectPool<NettyClient> masterClientPool = NettyPoolUtil.createNettyPoolWithGlobalResource(new DefaultEndPoint(master.getIp(), master.getPort()));
        InfoCommand infoCommand = new InfoCommand(masterClientPool, InfoCommand.INFO_TYPE.STATS, scheduled);
        String value = infoCommand.execute().get();
        Integer originFsync = new InfoResultExtractor(value).extractAsInteger("sync_full");

        logger.info("finish link ");

        assertGtid(master);
        assertReplOffset(master);

        CountDownLatch latch = new CountDownLatch(1);
        executors.execute(() -> {
            for(int i = 0; i < 20; i++) {
                sendMessageToMaster(master, 12);

            }
            latch.countDown();
        });

        executors.execute(() -> {
            try {
                logger.info("config set gtid-enabled no");
                setRedisToGtidNotEnabled(getRedisMaster().getIp(), getRedisMaster().getPort());
                for(RedisMeta slave : getRedisSlaves()) {
                    setRedisToGtidNotEnabled(slave.getIp(), slave.getPort());
                }
            } catch (Exception e) {
                logger.error("[setRedisToGtidNotEnabled]", e);
            }
        });

        Thread.sleep(1000);
        latch.await(10, TimeUnit.SECONDS);

        sendMesssageToMasterAndTest(10, getRedisMaster(), getRedisSlaves());

        infoCommand.reset();
        value = infoCommand.execute().get();
        Integer currentFsync = new InfoResultExtractor(value).extractAsInteger("sync_full");
        Assert.assertEquals(originFsync, currentFsync);
    }
}
