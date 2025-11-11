package com.ctrip.xpipe.redis.integratedtest.keeper;

import com.ctrip.xpipe.api.proxy.ProxyConnectProtocol;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoCommand;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import com.ctrip.xpipe.redis.keeper.impl.DefaultRedisKeeperServer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.stream.IntStream;

public class KeeperXSyncCrossRegionTest extends AbstractKeeperIntegratedMultiDcXsync {

    @Before
    public void setupKeeperXSyncCrossRegion() {
        for(DcMeta dcMeta : getDcMetas()){
            for(RedisMeta redisMeta : getDcRedises(dcMeta.getId(), getClusterId(), getShardId())){
                jedisExecCommand(redisMeta.getIp(), redisMeta.getPort(), "CONFIG", "SET", "gtid-enabled", "yes");
                jedisExecCommand(redisMeta.getIp(), redisMeta.getPort(), "CONFIG", "SET", "gtid-xsync-max-gap", "10000");
            }
        }
    }

    @Override
    protected KeeperConfig getKeeperConfig() {
        TestKeeperConfig config = (TestKeeperConfig) super.getKeeperConfig();
        config.setXsyncMaxGap(0);
        config.setXsyncMaxGap(10000);
        config.setReplicationStoreMaxCommandsToTransferBeforeCreateRdb(1024 * 1024);
        return config;
    }

    @Test
    public void testCrossRegionGap() throws Exception {
        RedisKeeperServer primaryDcKeeper = getRedisKeeperServerActive(getPrimaryDc());
        RedisKeeperServer backupDcKeeper = getRedisKeeperServerActive(getBackupDc());

        ((DefaultRedisKeeperServer) backupDcKeeper).setRedisMasterProtocol(Mockito.mock(ProxyConnectProtocol.class));
        RedisMeta redisMaster = getRedisMaster();
        RedisMeta backupDcSlave = getRedisSlaves(getBackupDc()).get(0);

        batchSet(redisMaster, "km_", "vm_", 0, 10);
        sleep(1000);
        GtidSet masterGtidSet = new GtidSet(infoRedis(redisMaster.getIp(), redisMaster.getPort(), InfoCommand.INFO_TYPE.GTID, "gtid_executed"));
        GtidSet masterLost = new GtidSet(infoRedis(redisMaster.getIp(), redisMaster.getPort(), InfoCommand.INFO_TYPE.GTID, "gtid_lost"));
        GtidSet slaveGtidSet = new GtidSet(infoRedis(backupDcSlave.getIp(), backupDcSlave.getPort(), InfoCommand.INFO_TYPE.GTID, "gtid_executed"));
        GtidSet slaveLost = new GtidSet(infoRedis(backupDcSlave.getIp(), backupDcSlave.getPort(), InfoCommand.INFO_TYPE.GTID, "gtid_lost"));
        Assert.assertEquals(masterGtidSet, slaveGtidSet);
        Assert.assertEquals(masterLost, slaveLost);
        Assert.assertTrue(masterLost.isEmpty());

        jedisExecCommand(backupDcSlave.getIp(), backupDcSlave.getPort(), "SLAVEOF", "NO", "ONE");
        batchSet(backupDcSlave, "ks_", "vs_", 0, 10);

        int fullSyncCnt = Integer.parseInt(infoRedis(redisMaster.getIp(), redisMaster.getPort(), InfoCommand.INFO_TYPE.STATS, "sync_full"));
        jedisExecCommand(backupDcSlave.getIp(), backupDcSlave.getPort(), "SLAVEOF", "127.0.0.1", ""+backupDcKeeper.getListeningPort());
        waitConditionUntilTimeOut(() -> {
            try {
                GtidSet gtidsetLost = new GtidSet(infoRedis(redisMaster.getIp(), redisMaster.getPort(), InfoCommand.INFO_TYPE.GTID, "gtid_lost"));
                return gtidsetLost.itemCnt() == 10;
            } catch (Exception e) {
                logger.info("[testCrossRegionGap]", e);
                return false;
            }
        });

        batchSet(redisMaster, "km_", "vm_", 10, 20);
        sleep(1000);
        masterGtidSet = new GtidSet(infoRedis(redisMaster.getIp(), redisMaster.getPort(), InfoCommand.INFO_TYPE.GTID, "gtid_executed"));
        masterLost = new GtidSet(infoRedis(redisMaster.getIp(), redisMaster.getPort(), InfoCommand.INFO_TYPE.GTID, "gtid_lost"));
        slaveGtidSet = new GtidSet(infoRedis(backupDcSlave.getIp(), backupDcSlave.getPort(), InfoCommand.INFO_TYPE.GTID, "gtid_executed"));
        slaveLost = new GtidSet(infoRedis(backupDcSlave.getIp(), backupDcSlave.getPort(), InfoCommand.INFO_TYPE.GTID, "gtid_lost"));
        Assert.assertEquals(masterGtidSet.union(masterLost), slaveGtidSet.union(slaveLost));
        Assert.assertTrue(slaveLost.isEmpty());
        Assert.assertEquals(fullSyncCnt, Integer.parseInt(infoRedis(redisMaster.getIp(), redisMaster.getPort(), InfoCommand.INFO_TYPE.STATS, "sync_full")));

        for (RedisMeta redisMeta: getRedisSlaves(getPrimaryDc())) {
            GtidSet primaryDcSlaveExecuted = new GtidSet(infoRedis(redisMeta.getIp(), redisMeta.getPort(), InfoCommand.INFO_TYPE.GTID, "gtid_executed"));
            GtidSet primaryDcSlaveLost = new GtidSet(infoRedis(redisMeta.getIp(), redisMeta.getPort(), InfoCommand.INFO_TYPE.GTID, "gtid_lost"));
            Assert.assertEquals(masterGtidSet, primaryDcSlaveExecuted);
            Assert.assertEquals(masterLost, primaryDcSlaveLost);
        }
    }

    private void batchSet(RedisMeta redis, String key_pre, String val_pre, int start, int end) {
        IntStream.range(start, end).forEach(i -> {
            jedisExecCommand(redis.getIp(), redis.getPort(), "SET", "key_pre" + i, "val_pre" + i);
        });
    }

}
