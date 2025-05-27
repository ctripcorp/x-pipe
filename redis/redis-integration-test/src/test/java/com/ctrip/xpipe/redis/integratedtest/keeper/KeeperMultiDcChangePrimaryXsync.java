package com.ctrip.xpipe.redis.integratedtest.keeper;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.netty.NettyPoolUtil;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService;
import com.ctrip.xpipe.redis.core.protocal.cmd.ConfigSetCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class KeeperMultiDcChangePrimaryXsync  extends AbstractKeeperIntegratedMultiDc {

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

        when(dcMetaCache.getShardRedises(getClusterDbId(), getShardDbId())).thenReturn(getDcRedises(backupDc, getClusterId(), getShardId()));
        when(currentMetaManager.getSurviveKeepers(getClusterDbId(), getShardDbId())).thenReturn(getDcKeepers(backupDc, getClusterId(), getShardId()));

        ClusterMeta clusterMeta = mock(ClusterMeta.class);
        when(clusterMeta.getActiveDc()).thenReturn(primaryDc);
        when(currentMetaManager.getClusterMeta(getClusterDbId())).thenReturn(clusterMeta);

        assertGtid(getRedisMaster());
        assertReplOffset(getRedisMaster());

        logger.info(remarkableMessage("[make dc primary]change dc primary to:" + backupDc));
        BecomePrimaryAction becomePrimaryAction = new BecomePrimaryAction(getClusterDbId(), getShardDbId(), dcMetaCache,
                currentMetaManager, sentinelManager, offsetWaiter, new ExecutionLog(currentTestName()),
                getXpipeNettyClientKeyedObjectPool(), newMasterChooser, scheduled, executors);
        MetaServerConsoleService.PrimaryDcChangeMessage message = becomePrimaryAction.changePrimaryDc(getClusterDbId(), getShardDbId(), backupDc, new MasterInfo());
        logger.info("{}", message);

        sleep(2000);

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

        sleep(2000);

        RedisMeta newRedisMaster = newMasterChooser.getLastChoosenMaster();
        setRedisToGtidEnabled(newRedisMaster.getIp(), newRedisMaster.getPort());
        logger.info("new master is {}", newRedisMaster);
        List<RedisMeta> allRedises = getRedises(primaryDc);
        allRedises.addAll(getRedises(backupDc));
        allRedises.remove(newRedisMaster);


        logger.info("{}\n{}", newRedisMaster, allRedises);

        sendMessageToMaster(newRedisMaster, 200);

        Thread.sleep(5000);

        assertGtid(newRedisMaster);
        assertReplOffset(newRedisMaster);
    }

    @Override
    protected KeeperConfig getKeeperConfig() {
        return new TestKeeperConfig(1 << 20, 100, 100 * (1 << 20), 2000);
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

    private void assertGtid(RedisMeta master) throws ExecutionException, InterruptedException {
        String masterGtid = getGtidSet(master.getIp(), master.getPort(), "gtid_set");
        String activeKeeperGtid = getGtidSet(getKeeperActive(getPrimaryDc()).getIp(), getKeeperActive(getPrimaryDc()).getPort(), "gtid_executed");
        String backGtidSet = getGtidSet(getKeeperActive(getBackupDc()).getIp(), getKeeperActive(getBackupDc()).getPort(), "gtid_executed");
        getOffset(getKeeperActive(getPrimaryDc()).getIp(), getKeeperActive(getPrimaryDc()).getPort());
        logger.info("masterGtid:{}", masterGtid);
        logger.info("activeKeeperGtid:{}", activeKeeperGtid);
        logger.info("backGtidSet:{}", backGtidSet);
        for(RedisMeta slave: getRedisSlaves()) {
            String slaveGtidStr = getGtidSet(slave.getIp(), slave.getPort(), "gtid_set");
            logger.info("slave {}:{} gtid set: {}", slave.getIp(), slave.getPort(), slaveGtidStr);
            Assert.assertEquals(masterGtid, slaveGtidStr);
        }

    }

    private Long getOffset(String ip, int port) throws ExecutionException, InterruptedException {
        SimpleObjectPool<NettyClient> masterClientPool = NettyPoolUtil.createNettyPoolWithGlobalResource(new DefaultEndPoint(ip, port));
        InfoCommand infoCommand = new InfoCommand(masterClientPool, InfoCommand.INFO_TYPE.REPLICATION, scheduled);
        String value = infoCommand.execute().get();
        logger.info("get gtid set from {}, {}, {}", ip, port, value);
        String gtidSet = new InfoResultExtractor(value).extract("master_repl_offset");
        return Long.parseLong(gtidSet);
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
