package com.ctrip.xpipe.redis.integratedtest.keeper;


import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class PsyncXsyncSwitchAlways extends AbstractCustomKeeperIntegratedMultiDcXsync{
    private int switchCount = 10;

    @Test
    public void testXsyncSwitchAlways() throws Exception {
        startKeepers();
        makeKeeperRight();
        checkAllMasterLinkStatus();
        for(int i = 0;i<switchCount;i++) {
            if(i % 2 == 0){
                setRedisToGtidNotEnabled(getRedisMaster().getIp(),getRedisMaster().getPort());
            }else{
                setRedisToGtidEnabled(getRedisMaster().getIp(),getRedisMaster().getPort());
            }
            sendMessageToMasterAndTestSlaveRedis(128);
        }
    }

    @Test
    public void testKeeperLostMaxGapZero_Fail() throws Exception{
        startKeepers();
        makeKeeperRight();
        checkAllMasterLinkStatus();

        setRedisToGtidEnabled(getRedisMaster().getIp(),getRedisMaster().getPort());
        sleep(100);

        sendMessageToMasterAndTestSlaveRedis(1);
        long fullSyncCount = getRedisKeeperServer(getKeeperActive("jq")).getKeeperMonitor().getKeeperStats().getFullSyncCount();
        logger.info("keeper lost before full sync count {}",fullSyncCount);

        RedisMeta activeDcSlave = getRedisSlaves("jq").get(0);
        setRedisToGtidEnabled(activeDcSlave.getIp(),activeDcSlave.getPort());
        slaveOfNoOne(activeDcSlave);
        sendMessage(activeDcSlave,1,"hahh");

        KeeperMeta activeDcKeeper = getKeeperActive("jq");
        slaveOf(activeDcSlave,activeDcKeeper);
        Set<String> keys = sendIncrementMessage(10);

        checkMasterGtidLost();
        checkAllMasterSlaveGtidSet();
        assertSpecifiedKeyRedisEquals(getRedisMaster(),getRedisSlaves(),keys);
        long fullSyncCount2 = getRedisKeeperServer(getKeeperActive("jq")).getKeeperMonitor().getKeeperStats().getFullSyncCount();
        logger.info("keeper lost after full sync count {}",fullSyncCount2);
        Assert.assertEquals(fullSyncCount2,fullSyncCount+2);
    }


    @Test
    public void testKeeperLostAfterRemoveNotExists() throws Exception{
        startKeepers();
        makeKeeperRight();
        checkAllMasterLinkStatus();

        setRedisToGtidEnabled(getRedisMaster().getIp(),getRedisMaster().getPort());
        sleep(100);

        sendMessageToMasterAndTestSlaveRedis(1);

        RedisMeta activeDcSlave = getRedisSlaves("jq").get(0);
        setRedisToGtidEnabled(activeDcSlave.getIp(),activeDcSlave.getPort());
        slaveOfNoOne(activeDcSlave);
        sendMessage(activeDcSlave,1,"hahh");

        KeeperMeta activeDcKeeper = getKeeperActive("jq");
        slaveOf(activeDcSlave,activeDcKeeper);
        checkMasterAndActiveKeeperGtidLostNotEmpty();
        
        GtidSet masterLostSet = gtidxRemoveLost(getRedisMaster().getIp(),getRedisMaster().getPort());
        gtidxAddExecuted(getRedisMaster().getIp(),getRedisMaster().getPort(),masterLostSet);

        GtidSet activeKeeperLostSet = gtidxRemoveLost(activeDcKeeper.getIp(),activeDcKeeper.getPort());
        gtidxAddExecuted(activeDcKeeper.getIp(),activeDcKeeper.getPort(),activeKeeperLostSet);

        setKeeperState(activeDcKeeper, KeeperState.ACTIVE,"127.0.0.1",0);
        sleep(2000);
        setKeeperState(activeDcKeeper, KeeperState.ACTIVE,getRedisMaster().getIp(),getRedisMaster().getPort());

        checkMasterAndActiveDcKeeperGtidLostEmpty();
        KeeperMeta backupDcKeeper = getKeeperActive("oy");
        GtidSet backupKeeperLostSet = gtidxRemoveLost(backupDcKeeper.getIp(),backupDcKeeper.getPort());
        gtidxAddExecuted(backupDcKeeper.getIp(),backupDcKeeper.getPort(),backupKeeperLostSet);

        setKeeperState(backupDcKeeper, KeeperState.ACTIVE,"127.0.0.1",0);
        sleep(2000);
        setKeeperState(backupDcKeeper, KeeperState.ACTIVE,activeDcKeeper.getIp(),activeDcKeeper.getPort());
        checkMasterAndKeeperGtidLostEmpty();
    }


    @Test
    public void testKeeperLostMaxGap_10000_Success() throws Exception{
        maxGap = 10000;
        startKeepers();

        makeKeeperRight();
        checkAllMasterLinkStatus();

        setRedisToGtidEnabled(getRedisMaster().getIp(),getRedisMaster().getPort());
        sleep(100);

        sendMessageToMasterAndTestSlaveRedis(1);
        long fullSyncCount = getRedisKeeperServer(getKeeperActive("jq")).getKeeperMonitor().getKeeperStats().getFullSyncCount();
        logger.info("keeper lost before full sync count {}",fullSyncCount);

        RedisMeta activeDcSlave = getRedisSlaves("jq").get(0);
        setRedisToGtidEnabled(activeDcSlave.getIp(),activeDcSlave.getPort());
        slaveOfNoOne(activeDcSlave);
        sendMessage(activeDcSlave,1,"hahh");

        KeeperMeta activeDcKeeper = getKeeperActive("jq");
        slaveOf(activeDcSlave,activeDcKeeper);

        Set<String> keys = sendIncrementMessage(10);

        checkMasterGtidLostNo();
        checkAllMasterSlaveGtidSet();
        assertSpecifiedKeyRedisEquals(getRedisMaster(),getRedisSlaves(),keys);
        long fullSyncCount2 = getRedisKeeperServer(getKeeperActive("jq")).getKeeperMonitor().getKeeperStats().getFullSyncCount();
        logger.info("keeper lost after full sync count {}",fullSyncCount2);

        Assert.assertEquals(fullSyncCount2,fullSyncCount);
    }


    @Test
    public void testKeeperRedisLostInit() throws Exception{
        for(RedisMeta slave : getAllRedisMaster()) {
            setRedisToGtidMaxGap(slave.getIp(), slave.getPort(),0);
        }
        setAllRedisMasterGtidEnabled();
        startKeepers();
        makeKeeperRight();
        checkAllMasterLinkStatus();
        sendMessage(getRedisMaster(),"hello","master");
        sendMessage(getAllRedisMaster().get(1),"hello","slave");
        sendMessage(getAllRedisMaster().get(2),"hello","sub-slave-1");
        sendMessage(getAllRedisMaster().get(3),"hello","sub-slave-2");

        KeeperMeta activeMeta = getKeeperActive(getRedisMaster());
        slaveOf(getAllRedisMaster().get(2),activeMeta);
        slaveOf(getAllRedisMaster().get(3),activeMeta);

        makePrimaryDcKeeperRight(getAllRedisMaster().get(1));

        checkAllRedisMasterGtidSet(getAllRedisMaster().get(1));
        List<RedisMeta> slaves = getAllRedisMaster();
        slaves.remove(getRedisMaster());
        assertSpecifiedKeyRedisEquals(getAllRedisMaster().get(1),slaves,Set.of("hello"));
    }

}
