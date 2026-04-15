package com.ctrip.xpipe.redis.console.notifier;

import com.ctrip.xpipe.api.migration.auto.MonitorService;
import com.ctrip.xpipe.api.migration.auto.data.MonitorGroupMeta;
import com.ctrip.xpipe.api.migration.auto.data.MonitorShardMeta;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.BeaconManager;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.core.beacon.BeaconSystem;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.migration.auto.MonitorManager;
import com.ctrip.xpipe.redis.console.service.meta.BeaconMetaService;
import com.ctrip.xpipe.redis.core.config.ConsoleCommonConfig;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.Arrays;
import java.util.Set;

/**
 * @author lishanglin
 * date 2021/1/19
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultClusterMonitorModifiedNotifierTest extends AbstractConsoleTest {

    @Mock
    private MonitorManager monitorManager;

    @Mock
    private BeaconMetaService beaconMetaService;

    @Mock
    private MonitorService monitorService;

    @Mock
    private ConsoleCommonConfig config;

    @Mock
    private MetaCache metaCache;

    @Mock
    private ConsoleConfig consoleConfig;

    private DefaultClusterMonitorModifiedNotifier notifier;

    @Before
    public void setupDefaultClusterMonitorModifiedNotifierTest() {
        notifier = new DefaultClusterMonitorModifiedNotifier(beaconMetaService, monitorManager, metaCache, config, consoleConfig);

        Mockito.when(monitorManager.get(Mockito.anyLong(), Mockito.anyString(), Mockito.any())).thenReturn(monitorService);
        Mockito.when(beaconMetaService.buildCurrentBeaconGroups(Mockito.anyString())).thenReturn(Collections.singleton(new MonitorGroupMeta()));
        Mockito.when(config.getBeaconSupportZones()).thenReturn(Collections.singleton("SHA"));
        Mockito.when(consoleConfig.supportSentinelBeacon(Mockito.anyLong(), Mockito.anyString())).thenReturn(false);
        Mockito.when(metaCache.getClusterType(Mockito.anyString())).thenReturn(ClusterType.ONE_WAY);
        Mockito.when(metaCache.getActiveDc(Mockito.anyString())).thenReturn("jq");
        Mockito.when(metaCache.isDcInRegion(Mockito.anyString(), Mockito.anyString())).thenReturn(true);
        Mockito.when(metaCache.getXpipeMeta()).thenReturn(mockXpipeMeta());
    }

    @Test
    public void testNotifyClusterChange() throws Exception {
        notifier.notifyClusterUpdate("cluster1", 1, "20201030");
        waitConditionUntilTimeOut(() -> Mockito.mockingDetails(monitorService).getInvocations().size() >= 1);

        Mockito.verify(beaconMetaService).buildCurrentBeaconGroups("cluster1");
        Mockito.verify(monitorService).registerCluster(BeaconSystem.getDefault().getSystemName(), "cluster1", Collections.singleton(new MonitorGroupMeta()),
                Collections.singletonMap(BeaconManager.EXTRA_LAST_MODIFY_TIME, "20201030"));
    }

    @Test
    public void testNotifyClusterDeleted() throws Exception {
        notifier.notifyClusterDelete("cluster1", 1);
        waitConditionUntilTimeOut(() -> Mockito.mockingDetails(monitorService).getInvocations().size() >= 1);

        Mockito.verify(monitorService).unregisterCluster(BeaconSystem.getDefault().getSystemName(), "cluster1");
    }

    @Test
    public void testNotifyClusterChangeForSentinelBeacon() throws Exception {
        Set<MonitorShardMeta> shards = Collections.singleton(new MonitorShardMeta("shard1", Arrays.asList(
                new MonitorGroupMeta("127.0.0.1:6379", "jq", Collections.emptySet(), true)
        )));
        Mockito.when(consoleConfig.supportSentinelBeacon(Mockito.anyLong(), Mockito.anyString())).thenReturn(true);
        Mockito.when(beaconMetaService.buildBeaconShards(Mockito.anyString(), Mockito.anyString())).thenReturn(shards);

        notifier.notifyClusterUpdate("cluster1", 1, "20201030");
        waitConditionUntilTimeOut(() -> Mockito.mockingDetails(monitorService).getInvocations().size() >= 1);

        Mockito.verify(beaconMetaService).buildCurrentBeaconGroups("cluster1");
        Mockito.verify(beaconMetaService).buildBeaconShards(Mockito.eq("cluster1"), Mockito.anyString());
        Mockito.verify(monitorService).registerCluster(BeaconSystem.getDefault().getSystemName(), "cluster1", Collections.singleton(new MonitorGroupMeta()),
                Collections.singletonMap(BeaconManager.EXTRA_LAST_MODIFY_TIME, "20201030"));
        Mockito.verify(monitorService).registerCluster(BeaconSystem.getDefault().getSystemName(), "cluster1",
                null, shards, Collections.singletonMap(BeaconManager.EXTRA_LAST_MODIFY_TIME, "20201030"));
    }

    @Test
    public void testClusterActiveDcMNotSupport() {
        Mockito.when(metaCache.isDcInRegion(Mockito.anyString(), Mockito.anyString())).thenReturn(false);

        notifier.notifyClusterUpdate("cluster1", 1, "20201030");
        notifier.notifyClusterDelete("cluster1", 1);
        Assert.assertEquals(0, Mockito.mockingDetails(monitorService).getInvocations().size());
    }

    @Test
    public void testSentinelNotifyEvenIfDrZoneNotSupport() throws Exception {
        Set<MonitorShardMeta> shards = Collections.singleton(new MonitorShardMeta("shard1", Arrays.asList(
                new MonitorGroupMeta("127.0.0.1:6379", "jq", Collections.emptySet(), true)
        )));
        Mockito.when(metaCache.isDcInRegion(Mockito.anyString(), Mockito.anyString())).thenReturn(false);
        Mockito.when(consoleConfig.supportSentinelBeacon(Mockito.anyLong(), Mockito.anyString())).thenReturn(true);
        Mockito.when(beaconMetaService.buildBeaconShards(Mockito.anyString(), Mockito.anyString())).thenReturn(shards);

        notifier.notifyClusterUpdate("cluster1", 1, "20201030");
        waitConditionUntilTimeOut(() -> Mockito.mockingDetails(monitorService).getInvocations().size() >= 1);

        Mockito.verify(beaconMetaService).buildBeaconShards(Mockito.eq("cluster1"), Mockito.anyString());
        Mockito.verify(monitorService).registerCluster(BeaconSystem.getDefault().getSystemName(), "cluster1",
                null, shards, Collections.singletonMap(BeaconManager.EXTRA_LAST_MODIFY_TIME, "20201030"));
    }

    private XpipeMeta mockXpipeMeta() {
        XpipeMeta xpipeMeta = new XpipeMeta();
        DcMeta dcMeta = new DcMeta("jq");
        dcMeta.addCluster(new ClusterMeta("cluster1").setType(ClusterType.ONE_WAY.name()).setActiveDc("jq").setOrgId(1));
        xpipeMeta.addDc(dcMeta);
        return xpipeMeta;
    }

}
