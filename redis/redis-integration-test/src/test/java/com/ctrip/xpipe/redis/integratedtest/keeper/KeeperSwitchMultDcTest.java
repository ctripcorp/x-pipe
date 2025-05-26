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

public class KeeperSwitchMultDcTest extends AbstractKeeperIntegratedMultiDc {

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

        KeeperMeta activeKeeperMeta = getKeeperActive(getPrimaryDc());
        KeeperMeta backupKeeperMeta = getKeepersBackup(getPrimaryDc()).iterator().next();

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
            sendMessageToMaster(master, 512); // may fsync if messages size over repl-backlog-size
            latch.countDown();
        });

        executors.execute(() -> {
            activeKeeperMeta.setActive(false);
            backupKeeperMeta.setActive(true);
            activeDcKeeperActive = backupKeeperMeta;

            try {
                makePrimaryDcKeeperRight();
                Thread.sleep(1000);
                makeBackupDcKeeperRight(getBackupDc());
                for(RedisMeta redisMeta : getRedisSlaves(getPrimaryDc())) {
                    setRedisMaster(redisMeta, new HostPort(activeDcKeeperActive.getIp(), activeDcKeeperActive.getPort()));
                }
            } catch (Exception e) {
                logger.info("[testSwitchOnWriting][adjust keeper fail]", e);
            }
        });

        Thread.sleep(1000);
        latch.await(10, TimeUnit.SECONDS);

        sendMessageToMaster(master, 10);

        Thread.sleep(5000);

        assertGtid(master);
        assertReplOffset(master);

        // sendMessageToMasterAndTestSlaveRedis(10);

        infoCommand.reset();
        value = infoCommand.execute().get();
        Integer currentFsync = new InfoResultExtractor(value).extractAsInteger("sync_full");
        Assert.assertEquals(originFsync, currentFsync);

    }


    @Test
    public void testSwitchMultDcOnWtringBackup() throws Exception {

        setRedisToGtidEnabled(getRedisMaster().getIp(), getRedisMaster().getPort());
        Thread.sleep(1000);

        for(KeeperMeta keeperMeta : getDcKeepers(getPrimaryDc(), getClusterId(), getShardId())) {
            waitConditionUntilTimeOut(() -> getRedisKeeperServer(keeperMeta).getRedisMaster().getMasterState().equals(REDIS_REPL_CONNECTED));
        }

        for(KeeperMeta keeperMeta : getDcKeepers(getBackupDc(), getClusterId(), getShardId())) {
            waitConditionUntilTimeOut(() -> getRedisKeeperServer(keeperMeta).getRedisMaster().getMasterState().equals(REDIS_REPL_CONNECTED));
        }

        KeeperMeta activeKeeperMeta = getKeeperActive(getBackupDc());
        KeeperMeta backupKeeperMeta = getKeepersBackup(getBackupDc()).iterator().next();

        Thread.sleep(1000);

        RedisMeta master = getRedisMaster();
        SimpleObjectPool<NettyClient> masterClientPool = NettyPoolUtil.createNettyPoolWithGlobalResource(new DefaultEndPoint(master.getIp(), master.getPort()));
        InfoCommand infoCommand = new InfoCommand(masterClientPool, InfoCommand.INFO_TYPE.STATS, scheduled);
        String value = infoCommand.execute().get();
        Integer originFsync = new InfoResultExtractor(value).extractAsInteger("sync_full");


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
                makeBackupDcKeeperRight(getBackupDc());
                for(RedisMeta redisMeta : getRedisSlaves(getBackupDc())) {
                    setRedisMaster(redisMeta, new HostPort(backupKeeperMeta.getIp(), backupKeeperMeta.getPort()));
                }
            } catch (Exception e) {
                logger.info("[testSwitchOnWriting][adjust keeper fail]", e);
            }
        });

        Thread.sleep(1000);
        latch.await(10, TimeUnit.SECONDS);

        sendMessageToMaster(getRedisMaster(), 10);

        Thread.sleep(5000);
        assertGtid(master);
        assertReplOffset(master);

        //sendMessageToMasterAndTestSlaveRedis(10);

        infoCommand.reset();
        value = infoCommand.execute().get();
        Integer currentFsync = new InfoResultExtractor(value).extractAsInteger("sync_full");
        Assert.assertEquals(originFsync, currentFsync);

    }


    private void setRedisMaster(RedisMeta redis, HostPort redisMaster) throws Exception {
        SimpleObjectPool<NettyClient> slaveClientPool = NettyPoolUtil.createNettyPoolWithGlobalResource(new DefaultEndPoint(redis.getIp(), redis.getPort()));
        new SlaveOfCommand(slaveClientPool, redisMaster.getHost(), redisMaster.getPort(), scheduled).execute().get();
    }

    private void setRedisToGtidEnabled(String ip, Integer port) throws Exception {
        SimpleObjectPool<NettyClient> keyPool = getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint(ip, port));
        ConfigSetCommand.ConfigSetGtidEnabled configSetGtidEnabled = new ConfigSetCommand.ConfigSetGtidEnabled(true, keyPool, scheduled);
        String gtid = configSetGtidEnabled.execute().get().toString();
        System.out.println(gtid);
    }

    private String getGtidSet(String ip, int port, String key) throws ExecutionException, InterruptedException {
        SimpleObjectPool<NettyClient> masterClientPool = NettyPoolUtil.createNettyPoolWithGlobalResource(new DefaultEndPoint(ip, port));
        InfoCommand infoCommand = new InfoCommand(masterClientPool, InfoCommand.INFO_TYPE.GTID, scheduled);
        String value = infoCommand.execute().get();
        logger.info("get gtid set from {}, {}, {}", ip, port, value);
        String gtidSet = new InfoResultExtractor(value).extract(key);
        return gtidSet;
    }

    private Long getOffset(String ip, int port) throws ExecutionException, InterruptedException {
        SimpleObjectPool<NettyClient> masterClientPool = NettyPoolUtil.createNettyPoolWithGlobalResource(new DefaultEndPoint(ip, port));
        InfoCommand infoCommand = new InfoCommand(masterClientPool, InfoCommand.INFO_TYPE.REPLICATION, scheduled);
        String value = infoCommand.execute().get();
        logger.info("get gtid set from {}, {}, {}", ip, port, value);
        String gtidSet = new InfoResultExtractor(value).extract("master_repl_offset");
        return Long.parseLong(gtidSet);
    }

    private long getReplOffset(String ip, int port) throws Exception {
        String replOffset = getGtidSet(ip, port, "gtid_master_repl_offset");
        String delat = getGtidSet(ip, port, "gtid_reploff_delta");
        return Long.parseLong(replOffset) + Long.parseLong(delat);
    }

    private void assertGtid(RedisMeta master) throws ExecutionException, InterruptedException {
        String masterGtid = getGtidSet(master.getIp(), master.getPort(), "gtid_set");
        String activeKeeperGtid = getGtidSet(getKeeperActive(getPrimaryDc()).getIp(), getKeeperActive(getPrimaryDc()).getPort(), "gtid_executed");
        String backGtidSet = getGtidSet(getKeeperActive(getBackupDc()).getIp(), getKeeperActive(getBackupDc()).getPort(), "gtid_executed");
        logger.info("masterGtid:{}", masterGtid);
        logger.info("activeKeeperGtid:{}", activeKeeperGtid);
        logger.info("backGtidSet:{}", backGtidSet);
        // Assert.assertEquals(activeKeeperGtid, masterGtid);
        // Assert.assertEquals(masterGtid, backGtidSet);
        for(RedisMeta slave: getRedisSlaves()) {
            String slaveGtidStr = getGtidSet(slave.getIp(), slave.getPort(), "gtid_set");
            logger.info("slave {}:{} gtid set: {}", slave.getIp(), slave.getPort(), slaveGtidStr);
            Assert.assertEquals(masterGtid, slaveGtidStr);
        }
    }

    private void assertReplOffset(RedisMeta master) throws Exception {
        long masterOffset = getOffset(master.getIp(), master.getPort());
        for(RedisMeta slave: getRedisSlaves()) {
            long slaveOffset = getOffset(slave.getIp(), slave.getPort());
            logger.info("slave {}:{} gtid set: {}", slave.getIp(), slave.getPort(), slaveOffset);
            Assert.assertEquals(masterOffset, slaveOffset);
        }
    }
}
