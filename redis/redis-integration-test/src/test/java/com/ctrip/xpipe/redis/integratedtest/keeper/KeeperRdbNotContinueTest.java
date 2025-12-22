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
    public void testSwitchProtocolRdbStoreSyncFail() throws Exception {
        KeeperMeta backupDcKeeper = getKeeperActive("oy");
        RedisKeeperServer backupDcKeeperServer = getRedisKeeperServerActive("oy");
        RedisMeta backupDcSlave = getRedisSlaves("oy").get(0);

        sendMessageToMasterAndTestSlaveRedis(128);
        backupDcKeeperServer.getReplicationStore().gc();


        setRedisToGtidEnabled(getRedisMaster().getIp(), getRedisMaster().getPort());

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

}
