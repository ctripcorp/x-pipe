package com.ctrip.xpipe.redis.integratedtest.keeper;

import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService;
import com.ctrip.xpipe.redis.core.protocal.pojo.MasterInfo;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import com.ctrip.xpipe.redis.meta.server.dcchange.ExecutionLog;
import com.ctrip.xpipe.redis.meta.server.dcchange.OffsetWaiter;
import com.ctrip.xpipe.redis.meta.server.dcchange.SentinelManager;
import com.ctrip.xpipe.redis.meta.server.dcchange.impl.BecomeBackupAction;
import com.ctrip.xpipe.redis.meta.server.dcchange.impl.BecomePrimaryAction;
import com.ctrip.xpipe.redis.meta.server.dcchange.impl.FirstNewMasterChooser;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.redis.meta.server.multidc.MultiDcService;
import com.ctrip.xpipe.utils.StringUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class KeeperMultiDcChangePrimaryXsync extends AbstractKeeperIntegratedMultiDcXsync {

    @Mock
    public DcMetaCache dcMetaCache;

    @Mock
    public CurrentMetaManager currentMetaManager;

    @Mock
    private SentinelManager sentinelManager;

    @Mock
    private MultiDcService multiDcService;

    @Mock
    private OffsetWaiter offsetWaiter;

    private FirstNewMasterChooser newMasterChooser;

    @Before
    public void beforeKeeperMultiDcChangePrimary() throws Exception{

        newMasterChooser = new FirstNewMasterChooser(getXpipeNettyClientKeyedObjectPool(), scheduled, executors);

    }

    @Test
    public void testChangePrimary() throws Exception{

        String primaryDc = getPrimaryDc();
        String backupDc = getBackupDc();
        //change backup to primary
        logger.info("[gtid set enabled] {}:{}", getRedisMaster().getIp(), getRedisMaster().getPort());
        setRedisToGtidEnabled(getRedisMaster().getIp(), getRedisMaster().getPort());
        Thread.sleep(2000);
        sendMessageToMaster(getRedisMaster(), 10);
        makeBackupDcKeeperRight(getBackupDc());
        Thread.sleep(2000);

        for(RedisMeta slave : getRedisSlaves()) {
            setRedisToGtidEnabled(slave.getIp(), slave.getPort());
        }

        when(dcMetaCache.getShardRedises(getClusterDbId(), getShardDbId())).thenReturn(getDcRedises(backupDc, getClusterId(), getShardId()));
        when(currentMetaManager.getSurviveKeepers(getClusterDbId(), getShardDbId())).thenReturn(getDcKeepers(backupDc, getClusterId(), getShardId()));

        ClusterMeta clusterMeta = mock(ClusterMeta.class);
        when(clusterMeta.getActiveDc()).thenReturn(primaryDc);
        when(currentMetaManager.getClusterMeta(getClusterDbId())).thenReturn(clusterMeta);

        assertMultiDcGtid(getRedisMaster());
        assertReplOffset(getRedisMaster());

        logger.info(remarkableMessage("[make dc primary]change dc primary to:" + backupDc));
        BecomePrimaryAction becomePrimaryAction = new BecomePrimaryAction(getClusterDbId(), getShardDbId(), dcMetaCache,
                currentMetaManager, sentinelManager, offsetWaiter, new ExecutionLog(currentTestName()),
                getXpipeNettyClientKeyedObjectPool(), newMasterChooser, scheduled, executors);
        MetaServerConsoleService.PrimaryDcChangeMessage message = becomePrimaryAction.changePrimaryDc(getClusterDbId(), getShardDbId(), backupDc, new MasterInfo());
        logger.info("{}", message);

        logger.info(remarkableMessage("[make dc backup]change dc primary to:" + backupDc));
        when(dcMetaCache.getPrimaryDc(getClusterDbId(), getShardDbId())).thenReturn(backupDc);
        when(multiDcService.getActiveKeeper(backupDc, getClusterDbId(), getShardDbId())).thenReturn(getDcKeepers(backupDc, getClusterId(), getShardId()).get(0));

        when(dcMetaCache.getShardRedises(getClusterDbId(), getShardDbId())).thenReturn(getDcRedises(primaryDc, getClusterId(), getShardId()));
        when(currentMetaManager.getKeeperActive(getClusterDbId(), getShardDbId())).thenReturn(getKeeperActive(primaryDc));
        when(currentMetaManager.getSurviveKeepers(getClusterDbId(), getShardDbId())).thenReturn(getDcKeepers(primaryDc, getClusterId(), getShardId()));


        BecomeBackupAction becomeBackupAction = new BecomeBackupAction(getClusterDbId(), getShardDbId(), dcMetaCache,
                currentMetaManager, sentinelManager, new ExecutionLog(currentTestName()),
                getXpipeNettyClientKeyedObjectPool(), multiDcService, scheduled, executors);
        message = becomeBackupAction.changePrimaryDc(getClusterDbId(), getShardDbId(), backupDc, new MasterInfo());
        logger.info("becomeBackupAction {}", message);

        RedisMeta newRedisMaster = newMasterChooser.getLastChoosenMaster();
        logger.info("new master is {}", newRedisMaster);
        List<RedisMeta> allRedises = getRedises(primaryDc);
        allRedises.addAll(getRedises(backupDc));
        allRedises.remove(newRedisMaster);


        logger.info("{}\n{}", newRedisMaster, allRedises);
        Thread.sleep(5000);
        sendMessageToMaster(newRedisMaster, 200);

        Thread.sleep(1000);
        newRedisMaster.setMaster(null);

        assertMultiDcGtid(newRedisMaster);
        assertReplOffset(newRedisMaster);
    }

    @Test
    public void testBeforeChangePrimaryWriteMultiIdcRedisAndKeeperMaxGap_Zero() throws Exception{
        setRedisToGtidMaxGap(getRedisMaster().getIp(), getRedisMaster().getPort(),0);
        for(RedisMeta slave : getRedisSlaves()) {
            setRedisToGtidMaxGap(slave.getIp(), slave.getPort(),0);
        }
        testBeforeChangePrimaryWriteMultiIdc();
        assertMultiDcGtidLostEmpty(newMasterChooser.getLastChoosenMaster());
    }


    @Test
    public void testBeforeChangePrimaryWriteMultiIdcRedisMaxGap_10000AndKeeperMaxGap_Zero() throws Exception{
        testBeforeChangePrimaryWriteMultiIdc();
        assertMultiDcGtidLost(newMasterChooser.getLastChoosenMaster(),getRedisMaster());
        assertMultiDcGtidIncreaseConsistency(newMasterChooser.getLastChoosenMaster(),getRedisMaster());
    }


    @Test
    public void testBeforeChangePrimaryWriteMultiIdcPrimaryDcMaxGap_ZeroAndBackupDcMaxGap_10000() throws Exception{
        setRedisToGtidMaxGap(getRedisMaster().getIp(), getRedisMaster().getPort(),0);
        for(RedisMeta slave : getRedisSlaves(getPrimaryDc())) {
            setRedisToGtidMaxGap(slave.getIp(), slave.getPort(),0);
        }
        KeeperMeta keeper = getKeeperActive(getBackupDc());
        setKeeperGtidMaxMap(keeper,10000);

        testBeforeChangePrimaryWriteMultiIdc();
        assertMultiDcGtidLostEmpty(newMasterChooser.getLastChoosenMaster());
    }


    @Test
    public void testBeforeChangePrimaryWriteMultiIdcPrimaryDcMaxGap_10000AndBackupDcMaxGap_Zero() throws Exception{
        for(RedisMeta slave : getRedisSlaves(getBackupDc())) {
            setRedisToGtidMaxGap(slave.getIp(), slave.getPort(),0);
        }
        KeeperMeta keeper = getKeeperActive(getPrimaryDc());
        setKeeperGtidMaxMap(keeper,10000);

        testBeforeChangePrimaryWriteMultiIdc();
        assertMultiDcPrimaryDcGtidLost(newMasterChooser.getLastChoosenMaster(),getRedisMaster());
        assertMultiDcGtidAllConsistency(newMasterChooser.getLastChoosenMaster(),getRedisMaster());
    }

    private void assertMultiDcGtidIncreaseConsistency(RedisMeta master,RedisMeta oldMaster) throws ExecutionException, InterruptedException {
        String masterUUId = getGtidSet(master.getIp(), master.getPort(), "gtid_uuid");
        String slaveGtidExecutedStr = getGtidSet(oldMaster.getIp(), oldMaster.getPort(), "gtid_executed");
        GtidSet slaveGtidExecuted = new GtidSet(slaveGtidExecutedStr);
        Assert.assertEquals(masterUUId+":11-20",slaveGtidExecuted.getUUIDSet(masterUUId).toString());

        for(RedisMeta slave: getRedisSlaves(getPrimaryDc())) {
            slaveGtidExecutedStr = getGtidSet(slave.getIp(), slave.getPort(), "gtid_executed");
            slaveGtidExecuted = new GtidSet(slaveGtidExecutedStr);
            Assert.assertEquals(masterUUId+":11-20",slaveGtidExecuted.getUUIDSet(masterUUId).toString());
        }

        for(RedisMeta slave: getRedisSlaves(getBackupDc())) {
            slaveGtidExecutedStr = getGtidSet(slave.getIp(), slave.getPort(), "gtid_executed");
            slaveGtidExecuted = new GtidSet(slaveGtidExecutedStr);
            Assert.assertEquals(masterUUId+":1-20",slaveGtidExecuted.getUUIDSet(masterUUId).toString());
        }
    }

    private void assertMultiDcPrimaryDcGtidLost(RedisMeta master,RedisMeta oldMaster) throws Exception {
        String masterUUId = getGtidSet(master.getIp(), master.getPort(), "gtid_uuid");
        String oldMasterUUId = getGtidSet(oldMaster.getIp(), oldMaster.getPort(), "gtid_uuid");

        waitConditionUntilTimeOut(()->{
            String masterGtidLostStr = null;
            try {
                masterGtidLostStr = getGtidSet(master.getIp(), master.getPort(), "gtid_lost");
            } catch (Exception e) {
                return false;
            }
            return StringUtil.trimEquals(oldMasterUUId+":3-12",masterGtidLostStr);
        },100000);


        waitConditionUntilTimeOut(()->{
            String slaveGtidLostStr = null;
            try {
                slaveGtidLostStr = getGtidSet(oldMaster.getIp(), oldMaster.getPort(), "gtid_lost");
            } catch (Exception e) {
                return false;
            }
            return StringUtil.trimEquals("",slaveGtidLostStr);
        },100000);


        for(RedisMeta slave: getRedisSlaves(getPrimaryDc())) {
            waitConditionUntilTimeOut(()->{
                String slaveGtidLostStr = null;
                try {
                    slaveGtidLostStr = getGtidSet(slave.getIp(), slave.getPort(), "gtid_lost");
                } catch (Exception e) {
                    return false;
                }
                return StringUtil.trimEquals("",slaveGtidLostStr);
            },100000);
        }

        for(RedisMeta slave: getRedisSlaves(getBackupDc())) {
            waitConditionUntilTimeOut(()->{
                String slaveGtidLostStr = null;
                try {
                    slaveGtidLostStr = getGtidSet(slave.getIp(), slave.getPort(), "gtid_lost");
                } catch (Exception e) {
                    return false;
                }
                return StringUtil.trimEquals(oldMasterUUId+":3-12",slaveGtidLostStr);
            },100000);
        }
    }

    private void assertMultiDcGtidLost(RedisMeta master,RedisMeta oldMaster) throws Exception {
        String masterUUId = getGtidSet(master.getIp(), master.getPort(), "gtid_uuid");
        String oldMasterUUId = getGtidSet(oldMaster.getIp(), oldMaster.getPort(), "gtid_uuid");

        waitConditionUntilTimeOut(()->{
            String masterGtidLostStr = null;
            try {
                masterGtidLostStr = getGtidSet(master.getIp(), master.getPort(), "gtid_lost");
            } catch (Exception e) {
                return false;
            }
            return StringUtil.trimEquals(oldMasterUUId+":3-12",masterGtidLostStr);
        },100000);


        waitConditionUntilTimeOut(()->{
            String slaveGtidLostStr = null;
            try {
                slaveGtidLostStr = getGtidSet(oldMaster.getIp(), oldMaster.getPort(), "gtid_lost");
            } catch (Exception e) {
                return false;
            }
            return StringUtil.trimEquals(masterUUId+":1-10",slaveGtidLostStr);
        },100000);


        for(RedisMeta slave: getRedisSlaves(getPrimaryDc())) {
            waitConditionUntilTimeOut(()->{
                String slaveGtidLostStr = null;
                try {
                    slaveGtidLostStr = getGtidSet(slave.getIp(), slave.getPort(), "gtid_lost");
                } catch (Exception e) {
                    return false;
                }
                return StringUtil.trimEquals(masterUUId+":1-10",slaveGtidLostStr);
            },100000);
        }

        for(RedisMeta slave: getRedisSlaves(getBackupDc())) {
            waitConditionUntilTimeOut(()->{
                String slaveGtidLostStr = null;
                try {
                    slaveGtidLostStr = getGtidSet(slave.getIp(), slave.getPort(), "gtid_lost");
                } catch (Exception e) {
                    return false;
                }
                return StringUtil.trimEquals(oldMasterUUId+":3-12",slaveGtidLostStr);
            },100000);
        }
    }

    private void assertMultiDcGtidAllConsistency(RedisMeta master,RedisMeta oldMaster) throws ExecutionException, InterruptedException {
        String masterUUId = getGtidSet(master.getIp(), master.getPort(), "gtid_uuid");
        String slaveGtidExecutedStr = getGtidSet(oldMaster.getIp(), oldMaster.getPort(), "gtid_executed");
        GtidSet slaveGtidExecuted = new GtidSet(slaveGtidExecutedStr);
        Assert.assertEquals(masterUUId+":1-20",slaveGtidExecuted.getUUIDSet(masterUUId).toString());

        for(RedisMeta slave: getRedisSlaves(getPrimaryDc())) {
            slaveGtidExecutedStr = getGtidSet(slave.getIp(), slave.getPort(), "gtid_executed");
            slaveGtidExecuted = new GtidSet(slaveGtidExecutedStr);
            Assert.assertEquals(masterUUId+":1-20",slaveGtidExecuted.getUUIDSet(masterUUId).toString());
        }

        for(RedisMeta slave: getRedisSlaves(getBackupDc())) {
            slaveGtidExecutedStr = getGtidSet(slave.getIp(), slave.getPort(), "gtid_executed");
            slaveGtidExecuted = new GtidSet(slaveGtidExecutedStr);
            Assert.assertEquals(masterUUId+":1-20",slaveGtidExecuted.getUUIDSet(masterUUId).toString());
        }
    }

    private void testBeforeChangePrimaryWriteMultiIdc() throws Exception{

        String primaryDc = getPrimaryDc();
        String backupDc = getBackupDc();
        //change backup to primary
        logger.info("[gtid set enabled] {}:{}", getRedisMaster().getIp(), getRedisMaster().getPort());
        setRedisToGtidEnabled(getRedisMaster().getIp(), getRedisMaster().getPort());
        Thread.sleep(2000);
        sendMessageToMaster(getRedisMaster(), 1);
        makeBackupDcKeeperRight(getBackupDc());
        Thread.sleep(2000);


        for(RedisMeta slave : getRedisSlaves()) {
            setRedisToGtidEnabled(slave.getIp(), slave.getPort());
        }

        when(dcMetaCache.getShardRedises(getClusterDbId(), getShardDbId())).thenReturn(getDcRedises(backupDc, getClusterId(), getShardId()));
        when(currentMetaManager.getSurviveKeepers(getClusterDbId(), getShardDbId())).thenReturn(getDcKeepers(backupDc, getClusterId(), getShardId()));

        ClusterMeta clusterMeta = mock(ClusterMeta.class);
        when(clusterMeta.getActiveDc()).thenReturn(primaryDc);
        when(currentMetaManager.getClusterMeta(getClusterDbId())).thenReturn(clusterMeta);

        assertMultiDcGtid(getRedisMaster());
        assertReplOffset(getRedisMaster());

        logger.info(remarkableMessage("[make dc primary]change dc primary to:" + backupDc));
        BecomePrimaryAction becomePrimaryAction = new BecomePrimaryAction(getClusterDbId(), getShardDbId(), dcMetaCache,
                currentMetaManager, sentinelManager, offsetWaiter, new ExecutionLog(currentTestName()),
                getXpipeNettyClientKeyedObjectPool(), newMasterChooser, scheduled, executors);
        MetaServerConsoleService.PrimaryDcChangeMessage message = becomePrimaryAction.changePrimaryDc(getClusterDbId(), getShardDbId(), backupDc, new MasterInfo());
        logger.info("{}", message);

        RedisMeta newRedisMaster = newMasterChooser.getLastChoosenMaster();
        logger.info("new master is {}", newRedisMaster);

        sendMessageToMaster(getRedisMaster(), 5);

        sendMessageToMaster(newRedisMaster, 5);

        logger.info(remarkableMessage("[make dc backup]change dc primary to:" + backupDc));
        when(dcMetaCache.getPrimaryDc(getClusterDbId(), getShardDbId())).thenReturn(backupDc);
        when(multiDcService.getActiveKeeper(backupDc, getClusterDbId(), getShardDbId())).thenReturn(getDcKeepers(backupDc, getClusterId(), getShardId()).get(0));

        when(dcMetaCache.getShardRedises(getClusterDbId(), getShardDbId())).thenReturn(getDcRedises(primaryDc, getClusterId(), getShardId()));
        when(currentMetaManager.getKeeperActive(getClusterDbId(), getShardDbId())).thenReturn(getKeeperActive(primaryDc));
        when(currentMetaManager.getSurviveKeepers(getClusterDbId(), getShardDbId())).thenReturn(getDcKeepers(primaryDc, getClusterId(), getShardId()));


        BecomeBackupAction becomeBackupAction = new BecomeBackupAction(getClusterDbId(), getShardDbId(), dcMetaCache,
                currentMetaManager, sentinelManager, new ExecutionLog(currentTestName()),
                getXpipeNettyClientKeyedObjectPool(), multiDcService, scheduled, executors);
        message = becomeBackupAction.changePrimaryDc(getClusterDbId(), getShardDbId(), backupDc, new MasterInfo());
        logger.info("becomeBackupAction {}", message);

        List<RedisMeta> allRedises = getRedises(primaryDc);
        allRedises.addAll(getRedises(backupDc));
        allRedises.remove(newRedisMaster);

        logger.info("{}\n{}", newRedisMaster, allRedises);
        Thread.sleep(5000);
        sendMessageToMaster(newRedisMaster, 5);

        Thread.sleep(1000);
        newRedisMaster.setMaster(null);

        assertMultiDcGtid(newRedisMaster);
        assertReplOffset(newRedisMaster);
    }


    @Override
    protected KeeperConfig getKeeperConfig() {
        TestKeeperConfig config = new TestKeeperConfig(1 << 20, 100, 100 * (1 << 20), 2000);
        return config;
    }
}
