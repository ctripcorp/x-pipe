package com.ctrip.xpipe.redis.integratedtest.keeper;

import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.protocal.MASTER_STATE;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStore;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author lishanglin
 * date 2021/8/16
 */
public class PartialSyncForKeeperTest extends AbstractKeeperIntegratedMultiDc {

    @Override
    protected KeeperConfig getKeeperConfig() {
        TestKeeperConfig keeperConfig = new TestKeeperConfig();
        keeperConfig.setReplicationStoreCommandFileSize(256);
        keeperConfig.setReplicationStoreMaxCommandsToTransferBeforeCreateRdb(Integer.MAX_VALUE);
        keeperConfig.setReplicationStoreGcIntervalSeconds(1000000);
        keeperConfig.setReplKeepSecondsAfterDown(0); // clear store after restart
        return keeperConfig;
    }


    @Test
    public void testKeeperQuickPartialSyncWithoutFullData() throws Exception {
        KeeperMeta oyKeeperMeta1 = getKeeperActive("oy");
        KeeperMeta oyKeeperMeta2 = getKeepersBackup("oy").iterator().next();
        KeeperMeta jqKeeperMeta = getKeeperActive("jq");
        RedisKeeperServer redisKeeperServer1 = getRedisKeeperServer(oyKeeperMeta1);
        RedisKeeperServer redisKeeperServer2 = getRedisKeeperServer(oyKeeperMeta2);
        RedisKeeperServer jqRedisKeeperServer = getRedisKeeperServer(jqKeeperMeta);

        stopKeeper(redisKeeperServer2);
        sendMessageToMasterAndTestSlaveRedis(512);
        redisKeeperServer1.getReplicationStore().gc();
        jqRedisKeeperServer.getReplicationStore().gc();

        DefaultReplicationStore replicationStore = (DefaultReplicationStore)jqRedisKeeperServer.getReplicationStore();
        int originRdbDumpCnt = replicationStore.getRdbUpdateCount();
        RedisKeeperServer newRedisKeeperServer2 = startKeeper(oyKeeperMeta2, leaderElectorManagers.get("oy"));
        makeBackupDcKeeperRight("oy");

        waitConditionUntilTimeOut(() -> null !=newRedisKeeperServer2.getRedisMaster() && newRedisKeeperServer2.getRedisMaster().getMasterState() == MASTER_STATE.REDIS_REPL_CONNECTED);
        Assert.assertEquals(originRdbDumpCnt, replicationStore.getRdbUpdateCount()); // no need full data for keeper empty start

        oyKeeperMeta1.setActive(false);
        oyKeeperMeta2.setActive(true);

        makeBackupDcKeeperRight("oy");
        waitConditionUntilTimeOut(() -> {
            try {
                return getKeeperState(oyKeeperMeta2).isActive();
            } catch (Exception e) {
                return  false;
            }
        });

        sendMessageToMasterAndTestSlaveRedis(10);
        Assert.assertEquals(originRdbDumpCnt, replicationStore.getRdbUpdateCount()); // no need full data for redis request psync
    }

}
