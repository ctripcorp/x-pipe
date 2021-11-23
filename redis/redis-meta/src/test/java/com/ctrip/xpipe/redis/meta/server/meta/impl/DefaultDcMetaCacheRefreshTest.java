package com.ctrip.xpipe.redis.meta.server.meta.impl;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.core.console.ConsoleService;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.protocal.pojo.MasterInfo;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
import com.ctrip.xpipe.redis.meta.server.config.UnitTestServerConfig;
import com.ctrip.xpipe.redis.meta.server.dcchange.ExecutionLog;
import com.ctrip.xpipe.redis.meta.server.dcchange.OffsetWaiter;
import com.ctrip.xpipe.redis.meta.server.dcchange.impl.BecomePrimaryAction;
import com.ctrip.xpipe.redis.meta.server.job.BackupDcClusterShardAdjustJob;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.StringUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;

@RunWith(MockitoJUnitRunner.class)
public class DefaultDcMetaCacheRefreshTest extends AbstractMetaServerTest {

    private DefaultDcMetaCache dcMetaCache;

    @Mock
    private ConsoleService mockConsoleService;

    @Mock
    private CurrentMetaManager mockCurrentMetaManager;

    @Mock
    private MetaServerConfig mockMetaServerConfig;

    @Before
    public void beforeDefaultDcMetaCacheTest() throws Exception {
        dcMetaCache = new DefaultDcMetaCache();

        XpipeMeta xpipeMeta = getXpipeMeta();
        DcMeta dcMeta = (DcMeta) xpipeMeta.getDcs().values().toArray()[0];
        Mockito.when(mockConsoleService.getDcMeta(Mockito.anyString(), Mockito.anySet())).thenReturn(dcMeta);
        Mockito.when(mockMetaServerConfig.getOwnClusterType()).thenReturn(Collections.singleton(ClusterType.BI_DIRECTION.toString()));

        injectConsoleServiceInto(dcMetaCache);
        dcMetaCache.initialize();
    }

    private void injectConsoleServiceInto(DefaultDcMetaCache dcMetaCache) throws Exception {
        Field consoleField = DefaultDcMetaCache.class.getDeclaredField("consoleService");
        consoleField.setAccessible(true);
        consoleField.set(dcMetaCache, mockConsoleService);
        Field configField = DefaultDcMetaCache.class.getDeclaredField("metaServerConfig");
        configField.setAccessible(true);
        configField.set(dcMetaCache, mockMetaServerConfig);
    }

    @Test
    public void refreshDcMetaWithDcChangeTest() throws Exception {
        DcMeta origin = dcMetaCache.getDcMeta().getDcMeta();
        dcMetaCache.setMetaServerConfig(new UnitTestServerConfig());
        ClusterMeta clusterMeta = (ClusterMeta) origin.getClusters().values().toArray()[0];
        ShardMeta shardMeta = (ShardMeta) clusterMeta.getShards().values().toArray()[0];
        String newPrimaryDc = "newPrimaryDc";
        String newBackUpDcs = StringUtil.isEmpty(clusterMeta.getBackupDcs()) ? clusterMeta.getActiveDc()
                : clusterMeta.getActiveDc() + "," + clusterMeta.getBackupDcs();
        MasterInfo masterInfo = new MasterInfo();

        Mockito.when(mockConsoleService.getDcMeta(Mockito.anyString(), Mockito.anySet())).thenAnswer(invocationOnMock -> {
            sleep(1000);
            return origin;
        });

        ExecutionLog executionLog = new ExecutionLog("refreshDcMetaWithDcChangeTest");
        BecomePrimaryAction becomePrimaryAction =
                new CustomBecomePrimaryAction(clusterMeta.getId(), shardMeta.getId(), dcMetaCache, executionLog);

        CountDownLatch latch = new CountDownLatch(2);
        CyclicBarrier barrier = new CyclicBarrier(2);
        new Thread(() -> {
            try {
                barrier.await();
            } catch (Exception e) {
                logger.info("[refreshDcMetaWithDcChangeTest] barrier fail");
                e.printStackTrace();
            }

            dcMetaCache.run();
            latch.countDown();
        }).start();

        new Thread(() -> {
            try {
                barrier.await();
            } catch (Exception e) {
                logger.info("[refreshDcMetaWithDcChangeTest] barrier fail");
                e.printStackTrace();
            }

            sleep(200); // wait for dcMetaCache run
            becomePrimaryAction.changePrimaryDc(clusterMeta.getId(), shardMeta.getId(), newPrimaryDc, masterInfo);
            latch.countDown();
        }).start();

        latch.await(3000, TimeUnit.MILLISECONDS);
        Assert.assertEquals(newPrimaryDc.toLowerCase(), dcMetaCache.getPrimaryDc(clusterMeta.getId(), shardMeta.getId()));
    }

    @Test
    public void refreshDcMetaWithBackupDcAdjust() throws Exception {
        DcMeta origin = dcMetaCache.getDcMeta().getDcMeta();
        ClusterMeta clusterMeta = (ClusterMeta) origin.getClusters().values().toArray()[0];
        ShardMeta shardMeta = (ShardMeta) clusterMeta.getShards().values().toArray()[0];
        dcMetaCache.getDcMeta().update(clusterMeta.setActiveDc("another"));

        BackupDcClusterShardAdjustJob job = new BackupDcClusterShardAdjustJob(clusterMeta.getId(), shardMeta.getId(), dcMetaCache,
                mockCurrentMetaManager, null, null, null);
        job.execute().get();
        Mockito.verify(mockCurrentMetaManager, Mockito.times(1)).getKeeperActive(Mockito.anyString(), Mockito.anyString());

        job = new BackupDcClusterShardAdjustJob(clusterMeta.getId(), shardMeta.getId(), dcMetaCache,
                mockCurrentMetaManager, null, null, null);
        dcMetaCache.getDcMeta().update(clusterMeta.setActiveDc(dcMetaCache.getCurrentDc()));
        job.execute().get();
        Mockito.verify(mockCurrentMetaManager, Mockito.times(1)).getKeeperActive(Mockito.anyString(), Mockito.anyString());
    }

    private static class CustomBecomePrimaryAction extends BecomePrimaryAction {
        public CustomBecomePrimaryAction(String cluster, String shard, DcMetaCache dcMetaCache,  ExecutionLog executionLog) {
            super(cluster, shard, dcMetaCache, null, null, new OffsetWaiter() {
                        @Override
                        public boolean tryWaitfor(HostPort hostPort, MasterInfo masterInfo, ExecutionLog executionLog) {
                            return false;
                        }
                    }, executionLog,
                    null, null, null, null);
        }

        @Override
        protected Pair<String, Integer> chooseNewMaster(String clusterId, String shardId) {
            return new Pair<>("127.0.0.1", 6379);
        }

        @Override
        protected List<RedisMeta> getAllSlaves(Pair<String, Integer> newMaster, List<RedisMeta> shardRedises) {
            return Collections.emptyList();
        }

        @Override
        protected void makeRedisesOk(Pair<String, Integer> newMaster, List<RedisMeta> slaves) {
        }

        @Override
        protected void makeKeepersOk(String clusterId, String shardId, Pair<String, Integer> newMaster) {
        }

        @Override
        protected void changeSentinel(String clusterId, String shardId, Pair<String, Integer> newMaster) {
        }
    }

}
