package com.ctrip.xpipe.redis.integratedtest.keeper;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.gtid.GtidSet;
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

public class MasterSwitchMultDcTest extends AbstractKeeperIntegratedMultiDc {

    @Override
    protected KeeperConfig getKeeperConfig() {
        TestKeeperConfig keeperConfig = new TestKeeperConfig();
        keeperConfig.setReplicationStoreMaxCommandsToTransferBeforeCreateRdb(Integer.MAX_VALUE);
        keeperConfig.setReplicationStoreGcIntervalSeconds(1000);
        keeperConfig.setReplicationStoreCommandFileSize(1024);
        return keeperConfig;
    }

    @Test
    public void testSwitchMultDcOnWtringPrimary() throws Exception {

        setRedisToGtidEnabled(getRedisMaster().getIp(), getRedisMaster().getPort());

        for(KeeperMeta keeperMeta : getDcKeepers(getPrimaryDc(), getClusterId(), getShardId())) {
            waitConditionUntilTimeOut(() -> getRedisKeeperServer(keeperMeta).getRedisMaster().getMasterState().equals(REDIS_REPL_CONNECTED));
        }

        for(KeeperMeta keeperMeta : getDcKeepers(getBackupDc(), getClusterId(), getShardId())) {
            waitConditionUntilTimeOut(() -> getRedisKeeperServer(keeperMeta).getRedisMaster().getMasterState().equals(REDIS_REPL_CONNECTED));
        }

        sendMessageToMaster(getRedisMaster(), 10);

        Thread.sleep(2000);

        RedisMeta master = getRedisMaster();
        RedisMeta newMaster = getRedisSlaves(getPrimaryDc()).iterator().next();

        logger.info("[new master]{}:{}", newMaster.getIp(), newMaster.getPort());
        assertGtid(master);
        assertReplOffset(master);

        logger.info("begin awitch primary dc master");


        CountDownLatch latch = new CountDownLatch(1);

        executors.execute(() -> {
            for(int i = 0; i < 50; i++) {
                RedisMeta mst = getRedisMaster();
                logger.info("send data to {}:{}", mst.getIp(), mst.getPort());
                try {
                    sendMessageToMaster(mst, 10);
                } catch (Exception e) {
                    logger.error("[sendDataToMaster]", e);
                }
            }
            latch.countDown();
        });

        executors.execute(() -> {
            try {
                KeeperMeta activeKeeperMeta = getKeeperActive(getPrimaryDc());
                master.setMaster(activeKeeperMeta.getIp() + ":" + activeKeeperMeta.getPort());
                newMaster.setMaster(null);
                setRedisMaster(newMaster, new HostPort(null, 6379));
                setRedisToGtidEnabled(newMaster.getIp(), newMaster.getPort());
                makePrimaryDcKeeperRight(newMaster);
                for(RedisMeta slave : getRedisSlaves(getPrimaryDc())) {
                    setRedisMaster(slave, new HostPort(activeKeeperMeta.getIp(), activeKeeperMeta.getPort()));
                }

                setRedisMaster(master, new HostPort(activeKeeperMeta.getIp(), activeKeeperMeta.getPort()));

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        Thread.sleep(2000);
        latch.await(10, TimeUnit.SECONDS);
        sendMessageToMaster(newMaster, 40);
        Thread.sleep(2000);
        assertGtid(newMaster);
        assertReplOffset(newMaster);
    }

    private Long getOffset(String ip, int port) throws ExecutionException, InterruptedException {
        SimpleObjectPool<NettyClient> masterClientPool = NettyPoolUtil.createNettyPoolWithGlobalResource(new DefaultEndPoint(ip, port));
        InfoCommand infoCommand = new InfoCommand(masterClientPool, InfoCommand.INFO_TYPE.REPLICATION, scheduled);
        String value = infoCommand.execute().get();
        logger.info("get gtid set from {}, {}, {}", ip, port, value);
        String gtidSet = new InfoResultExtractor(value).extract("master_repl_offset");
        return Long.parseLong(gtidSet);
    }

    private void assertGtid(RedisMeta master) throws ExecutionException, InterruptedException {
        String masterGtid = getGtidSet(master.getIp(), master.getPort(), "gtid_set");
        String activeKeeperGtid = getGtidSet(getKeeperActive(getPrimaryDc()).getIp(), getKeeperActive(getPrimaryDc()).getPort(), "gtid_executed");
        String backGtidSet = getGtidSet(getKeeperActive(getBackupDc()).getIp(), getKeeperActive(getBackupDc()).getPort(), "gtid_executed");
        logger.info("masterGtid:{}", masterGtid);
        logger.info("activeKeeperGtid:{}", activeKeeperGtid);
        logger.info("backGtidSet:{}", backGtidSet);
        for(RedisMeta slave: getRedisSlaves()) {
            String slaveGtidStr = getGtidSet(slave.getIp(), slave.getPort(), "gtid_set");
            logger.info("slave {}:{} gtid set: {}", slave.getIp(), slave.getPort(), slaveGtidStr);
            Assert.assertEquals(new GtidSet(masterGtid), new GtidSet(slaveGtidStr));
        }
    }

}
