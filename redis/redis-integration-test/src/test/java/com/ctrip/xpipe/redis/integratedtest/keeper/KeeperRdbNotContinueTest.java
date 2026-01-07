package com.ctrip.xpipe.redis.integratedtest.keeper;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.netty.NettyPoolUtil;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import com.ctrip.xpipe.redis.core.protocal.cmd.SlaveOfCommand;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author lishanglin
 * date 2021/7/21
 */
public class KeeperRdbNotContinueTest extends AbstractKeeperIntegratedMultiDc {

    @Override
    protected KeeperConfig getKeeperConfig() {
        TestKeeperConfig keeperConfig = new TestKeeperConfig();
        keeperConfig.setReplicationStoreCommandFileSize(256);
        keeperConfig.setReplicationStoreMaxCommandsToTransferBeforeCreateRdb(Integer.MAX_VALUE);
        keeperConfig.setReplicationStoreGcIntervalSeconds(1000000);
        return keeperConfig;
    }

    @Test
    public void testStoreReset() throws Exception {
        KeeperMeta backupDcKeeper = getKeeperActive("oy");
        RedisKeeperServer backupDcKeeperServer = getRedisKeeperServerActive("oy");
        RedisMeta backupDcSlave = getRedisSlaves("oy").get(0);

        sendMessageToMasterAndTestSlaveRedis(128);
        backupDcKeeperServer.getReplicationStore().gc();

        SimpleObjectPool<NettyClient> slaveClientPool = NettyPoolUtil.createNettyPoolWithGlobalResource(new DefaultEndPoint(backupDcSlave.getIp(), backupDcSlave.getPort()));
        new SlaveOfCommand(slaveClientPool, scheduled).execute().get();
        new SlaveOfCommand(slaveClientPool, backupDcKeeper.getIp(), backupDcKeeper.getPort(), scheduled).execute().get();

        waitConditionUntilTimeOut(() -> {
            try {
                String info = new InfoCommand(slaveClientPool, InfoCommand.INFO_TYPE.REPLICATION, scheduled).execute().get();
                InfoResultExtractor extractor = new InfoResultExtractor(info);
                return extractor.extract("master_link_status").equalsIgnoreCase("up");
            } catch (Exception e) {
                return false;
            }
        }, 10000, 1000);

        sendMessageToMasterAndTestSlaveRedis(128);
    }


    @Test
    public void testBackupDcPSYNCKeeperRDBStoreFailWhenSwitchXSYNC() throws Exception {
        KeeperMeta backupDcKeeper = getKeeperActive("oy");
        RedisKeeperServer backupDcKeeperServer = getRedisKeeperServerActive("oy");
        RedisKeeperServer activeDcKeeperServer = getRedisKeeperServerActive("jq");

        RedisMeta backupDcSlave = getRedisSlaves("oy").get(0);
        RedisMeta activeDcSlave = getRedisSlaves("jq").get(0);


        SimpleObjectPool<NettyClient> activeDcSlaveClientPool = NettyPoolUtil.createNettyPoolWithGlobalResource(new DefaultEndPoint(activeDcSlave.getIp(), activeDcSlave.getPort()));
        waitConditionUntilTimeOut(() -> {
            try {
                String info = new InfoCommand(activeDcSlaveClientPool, InfoCommand.INFO_TYPE.REPLICATION, scheduled).execute().get();
                InfoResultExtractor extractor = new InfoResultExtractor(info);
                return extractor.extract("master_link_status").equalsIgnoreCase("up");
            } catch (Exception e) {
                return false;
            }
        }, 30000, 1000);


        SimpleObjectPool<NettyClient> backupDcKeeperSlaveClientPool = NettyPoolUtil.createNettyPoolWithGlobalResource(new DefaultEndPoint(backupDcKeeper.getIp(), backupDcKeeper.getPort()));
        waitConditionUntilTimeOut(() -> {
            try {
                String info = new InfoCommand(backupDcKeeperSlaveClientPool, InfoCommand.INFO_TYPE.REPLICATION, scheduled).execute().get();
                InfoResultExtractor extractor = new InfoResultExtractor(info);
                return extractor.extract("master_link_status").equalsIgnoreCase("up");
            } catch (Exception e) {
                return false;
            }
        }, 30000, 1000);


        SimpleObjectPool<NettyClient> slaveClientPool = NettyPoolUtil.createNettyPoolWithGlobalResource(new DefaultEndPoint(backupDcSlave.getIp(), backupDcSlave.getPort()));
        waitConditionUntilTimeOut(() -> {
            try {
                String info = new InfoCommand(slaveClientPool, InfoCommand.INFO_TYPE.REPLICATION, scheduled).execute().get();
                InfoResultExtractor extractor = new InfoResultExtractor(info);
                return extractor.extract("master_link_status").equalsIgnoreCase("up");
            } catch (Exception e) {
                return false;
            }
        }, 30000, 1000);

        Assert.assertNotNull(activeDcKeeperServer.getReplicationStore().getMetaStore().dupReplicationStoreMeta().getRdbFile());

        Assert.assertEquals(4,activeDcKeeperServer.getKeeperMonitor().getKeeperStats().getFullSyncCount());
        waitConditionUntilTimeOut(() -> {
            try {
                String info = new InfoCommand(slaveClientPool, InfoCommand.INFO_TYPE.GTID, scheduled).execute().get();
                InfoResultExtractor extractor = new InfoResultExtractor(info);
                return extractor.extract("gtid_repl_mode").equalsIgnoreCase("psync");
            } catch (Exception e) {
                return false;
            }
        }, 30000, 1000);

        sendMessageToMasterAndTestSlaveRedis(128);

        backupDcKeeperServer.releaseRdb();
        backupDcKeeperServer.getReplicationStore().gc();


        setRedisToGtidEnabled(getRedisMaster().getIp(), getRedisMaster().getPort());

        waitConditionUntilTimeOut(() -> {
            try {
                String info = new InfoCommand(slaveClientPool, InfoCommand.INFO_TYPE.GTID, scheduled).execute().get();
                InfoResultExtractor extractor = new InfoResultExtractor(info);
                return extractor.extract("gtid_repl_mode").equalsIgnoreCase("xsync");
            } catch (Exception e) {
                return false;
            }
        }, 30000, 1000);

        new SlaveOfCommand(slaveClientPool, scheduled).execute().get();

        new SlaveOfCommand(slaveClientPool, backupDcKeeper.getIp(), backupDcKeeper.getPort(), scheduled).execute().get();


        waitConditionUntilTimeOut(() -> {
            try {
                String info = new InfoCommand(slaveClientPool, InfoCommand.INFO_TYPE.REPLICATION, scheduled).execute().get();
                InfoResultExtractor extractor = new InfoResultExtractor(info);
                return extractor.extract("master_link_status").equalsIgnoreCase("up");
            } catch (Exception e) {
                return false;
            }
        }, 30000, 1000);
        Assert.assertEquals(6,activeDcKeeperServer.getKeeperMonitor().getKeeperStats().getFullSyncCount());

        sendMessageToMasterAndTestSlaveRedis(128);
    }


}
