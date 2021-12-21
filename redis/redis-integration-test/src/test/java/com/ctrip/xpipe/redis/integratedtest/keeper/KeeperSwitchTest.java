package com.ctrip.xpipe.redis.integratedtest.keeper;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.netty.NettyPoolUtil;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import com.ctrip.xpipe.redis.core.protocal.cmd.SlaveOfCommand;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author lishanglin
 * date 2021/8/23
 */
public class KeeperSwitchTest extends AbstractKeeperIntegratedSingleDc {

    @Override
    protected KeeperConfig getKeeperConfig() {
        TestKeeperConfig keeperConfig = new TestKeeperConfig();
        keeperConfig.setReplicationStoreMaxCommandsToTransferBeforeCreateRdb(Integer.MAX_VALUE);
        keeperConfig.setReplicationStoreGcIntervalSeconds(1000000);
        return keeperConfig;
    }

    @Test
    public void testSwitchOnWriting() throws Exception {
        KeeperMeta activeKeeperMeta = getKeeperActive();
        KeeperMeta backupKeeperMeta = getKeepersBackup().iterator().next();

        RedisMeta master = getRedisMaster();
        SimpleObjectPool<NettyClient> masterClientPool = NettyPoolUtil.createNettyPoolWithGlobalResource(new DefaultEndPoint(master.getIp(), master.getPort()));
        InfoCommand infoCommand = new InfoCommand(masterClientPool, InfoCommand.INFO_TYPE.STATS, scheduled);
        String value = infoCommand.execute().get();
        Integer originFsync = new InfoResultExtractor(value).extractAsInteger("sync_full");

        CountDownLatch latch = new CountDownLatch(1);
        executors.execute(() -> {
            sendMessageToMaster(master, 512); // may fsync if messages size over repl-backlog-size
            latch.countDown();
        });


        executors.execute(() -> {
            activeKeeperMeta.setActive(false);
            backupKeeperMeta.setActive(true);

            try {
                makeKeeperRight();
                for (RedisMeta slave: getRedisSlaves()) {
                    setRedisMaster(slave, new HostPort(backupKeeperMeta.getIp(), backupKeeperMeta.getPort()));
                }
            } catch (Exception e) {
                logger.info("[testSwitchOnWriting][adjust keeper fail]", e);
            }
        });

        latch.await(5, TimeUnit.SECONDS);
        sendMessageToMasterAndTestSlaveRedis(10);

        infoCommand.reset();
        value = infoCommand.execute().get();
        Integer currentFsync = new InfoResultExtractor(value).extractAsInteger("sync_full");
        Assert.assertEquals(originFsync, currentFsync);
    }

    private void setRedisMaster(RedisMeta redis, HostPort redisMaster) throws Exception {
        SimpleObjectPool<NettyClient> slaveClientPool = NettyPoolUtil.createNettyPoolWithGlobalResource(new DefaultEndPoint(redis.getIp(), redis.getPort()));
        new SlaveOfCommand(slaveClientPool, redisMaster.getHost(), redisMaster.getPort(), scheduled).execute().get();
    }

}
