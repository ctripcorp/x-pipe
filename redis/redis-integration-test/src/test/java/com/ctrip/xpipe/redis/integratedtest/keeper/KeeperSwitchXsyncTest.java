package com.ctrip.xpipe.redis.integratedtest.keeper;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.netty.NettyPoolUtil;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.protocal.cmd.ConfigSetCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import com.ctrip.xpipe.redis.core.protocal.cmd.SlaveOfCommand;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.ctrip.xpipe.redis.core.protocal.MASTER_STATE.REDIS_REPL_CONNECTED;

public class KeeperSwitchXsyncTest extends AbstractKeeperIntegratedSingleDc {

    @Override
    protected KeeperConfig getKeeperConfig() {
        TestKeeperConfig keeperConfig = new TestKeeperConfig();
        keeperConfig.setReplicationStoreMaxCommandsToTransferBeforeCreateRdb(Integer.MAX_VALUE);
        keeperConfig.setReplicationStoreGcIntervalSeconds(1000000);
        keeperConfig.setReplicationStoreCommandFileSize(1024);
        return keeperConfig;
    }

    @Test
    public void testSwitchOnWriting() throws Exception {

        setRedisToGtidEnabled(redisMaster.getIp(), redisMaster.getPort());

        KeeperMeta activeKeeperMeta = getKeeperActive();
        KeeperMeta backupKeeperMeta = getKeepersBackup().iterator().next();


        waitConditionUntilTimeOut(() -> getRedisKeeperServer(activeKeeperMeta).getRedisMaster().getMasterState().equals(REDIS_REPL_CONNECTED));
        waitConditionUntilTimeOut(() -> getRedisKeeperServer(backupKeeperMeta).getRedisMaster().getMasterState().equals(REDIS_REPL_CONNECTED));

        RedisMeta master = getRedisMaster();
        SimpleObjectPool<NettyClient> masterClientPool = NettyPoolUtil.createNettyPoolWithGlobalResource(new DefaultEndPoint(master.getIp(), master.getPort()));
        InfoCommand infoCommand = new InfoCommand(masterClientPool, InfoCommand.INFO_TYPE.STATS, scheduled);
        String value = infoCommand.execute().get();
        Integer originFsync = new InfoResultExtractor(value).extractAsInteger("sync_full");

        Thread.sleep(2000);
        sendMessageToMasterAndTestSlaveRedis(10);
        logger.info("finish link ");

        assertGtid(master);
        assertReplOffset(master);

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
                Thread.sleep(500);
                for (RedisMeta slave: getRedisSlaves()) {
                    setRedisToGtidEnabled(slave.getIp(), slave.getPort());
                    setRedisMaster(slave, new HostPort(backupKeeperMeta.getIp(), backupKeeperMeta.getPort()));
                }
            } catch (Exception e) {
                logger.info("[testSwitchOnWriting][adjust keeper fail]", e);
            }
        });

        Thread.sleep(5000);
        latch.await(10, TimeUnit.SECONDS);


        assertGtid(master);
        assertReplOffset(master);

        sendMessageToMasterAndTestSlaveRedis(10);

        infoCommand.reset();
        value = infoCommand.execute().get();
        Integer currentFsync = new InfoResultExtractor(value).extractAsInteger("sync_full");
        Assert.assertEquals(originFsync, currentFsync);
    }

    private void assertGtid(RedisMeta master) throws ExecutionException, InterruptedException {
        String masterGtid = getGtidSet(master.getIp(), master.getPort(), "gtid_set");
        String activeKeeperGtid = getGtidSet(activeKeeper.getIp(),activeKeeper.getPort(), "gtid_executed");
        String backGtidSet = getGtidSet(backupKeeper.getIp(),backupKeeper.getPort(), "gtid_executed");
        logger.info("masterGtid:{}", masterGtid);
        logger.info("activeKeeperGtid:{}", activeKeeperGtid);
        logger.info("backGtidSet:{}", backGtidSet);
        for(RedisMeta slave: getRedisSlaves()) {
            String slaveGtidStr = getGtidSet(slave.getIp(), slave.getPort(), "gtid_set");
            logger.info("slave {}:{} gtid set: {}", slave.getIp(), slave.getPort(), slaveGtidStr);
            Assert.assertEquals(masterGtid, slaveGtidStr);
        }

    }

}
