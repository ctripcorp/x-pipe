package com.ctrip.xpipe.redis.console.healthcheck.nonredis.beacon;

import com.ctrip.xpipe.api.migration.auto.MonitorService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.migration.auto.DefaultMonitorClusterManager;
import com.ctrip.xpipe.redis.console.migration.auto.DefaultMonitorManager;
import com.ctrip.xpipe.redis.core.beacon.BeaconSystem;
import com.ctrip.xpipe.redis.console.migration.auto.MonitorManager;
import com.ctrip.xpipe.redis.core.config.ConsoleCommonConfig;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.anyString;

/**
 * @author lishanglin
 * date 2021/1/19
 */
@RunWith(MockitoJUnitRunner.Silent.class)
public class BeaconClusterMonitorCheckTest extends AbstractConsoleTest {

    @InjectMocks
    private BeaconClusterMonitorCheck check;

    @Mock
    private MonitorManager monitorManager;

    @Mock
    private MetaCache metaCache;

    @Mock
    private AlertManager alertManager;

    @Mock
    private MonitorService monitorService0;

    @Mock
    private MonitorService monitorService1;

    @Mock
    private ConsoleCommonConfig config;

    @Mock
    private ConsoleConfig consoleConfig;

    private  Map<Long, List<MonitorService>> getOrgServicesMap() {
        Map<Long, List<MonitorService>> orgServicesMap = new HashMap<>();
        orgServicesMap.put(0L, Collections.singletonList(monitorService0));
        orgServicesMap.put(1L, Collections.singletonList(monitorService1));
        return orgServicesMap;
    }

    @Before
    public void setupBeaconClusterMonitorCheckTest() throws NoSuchFieldException, IllegalAccessException {
        Mockito.when(monitorManager.getAllServices()).thenReturn(getOrgServicesMap());
        Mockito.when(metaCache.getXpipeMeta()).thenReturn(mockXPipeMeta());
        Mockito.when(config.getBeaconSupportZones()).thenReturn(Collections.emptySet());
        Mockito.when(config.monitorUnregisterProtectCount()).thenReturn(10);
        Mockito.when(consoleConfig.getServerMode()).thenReturn("CHECKER");
        Map<BeaconSystem, Map<Long, Set<String>>> mockmap = mockClustersByBeaconSystemOrg(getOrgServicesMap(), metaCache, config);
        Mockito.when(monitorManager.clustersByBeaconSystemOrg()).thenReturn(mockmap);
    }

    @Test
    public void testDoCheck() {
        Mockito.when(monitorService0.fetchAllClusters(BeaconSystem.XPIPE_ONE_WAY.getSystemName())).thenReturn(Sets.newHashSet("cluster2","hetero-cluster2"));
        Mockito.when(monitorService1.fetchAllClusters(BeaconSystem.XPIPE_ONE_WAY.getSystemName())).thenReturn(Sets.newHashSet("cluster1","hetero-cluster1"));
        Mockito.when(monitorService0.fetchAllClusters(BeaconSystem.XPIPE_BI_DIRECTION.getSystemName())).thenReturn(Sets.newHashSet("bi-cluster2"));
        Mockito.when(monitorService1.fetchAllClusters(BeaconSystem.XPIPE_BI_DIRECTION.getSystemName())).thenReturn(Sets.newHashSet("bi-cluster1"));
        check.doAction();

        Mockito.verify(monitorService0, Mockito.timeout(1000)).fetchAllClusters(BeaconSystem.XPIPE_ONE_WAY.getSystemName());
        Mockito.verify(monitorService1, Mockito.timeout(1000)).fetchAllClusters(BeaconSystem.XPIPE_ONE_WAY.getSystemName());
        Mockito.verify(monitorService0, Mockito.timeout(1000)).fetchAllClusters(BeaconSystem.XPIPE_BI_DIRECTION.getSystemName());
        Mockito.verify(monitorService1, Mockito.timeout(1000)).fetchAllClusters(BeaconSystem.XPIPE_BI_DIRECTION.getSystemName());
        Mockito.verify(monitorService0, Mockito.never()).unregisterCluster(anyString(), anyString());
        Mockito.verify(monitorService1, Mockito.never()).unregisterCluster(anyString(), anyString());
    }

    @Test
    public void testTooManyClusterNeedExcluded() {

        Mockito.when(config.monitorUnregisterProtectCount()).thenReturn(1);
        Set<String> oneWayNeedExcludeClusters = Sets.newHashSet("clusterx", "clustery");
        Set<String> biNeedExcludeClusters = Sets.newHashSet("bi-clusterx", "bi-clustery");
        Mockito.when(monitorService0.fetchAllClusters(BeaconSystem.XPIPE_ONE_WAY.getSystemName())).thenReturn(Sets.newHashSet("clusterx", "clustery"));
        Mockito.when(monitorService0.fetchAllClusters(BeaconSystem.XPIPE_BI_DIRECTION.getSystemName())).thenReturn(Sets.newHashSet("bi-clusterx", "bi-clustery"));

        check.doAction();
        Mockito.verify(monitorService0, Mockito.timeout(1000)).fetchAllClusters(BeaconSystem.XPIPE_ONE_WAY.getSystemName());
        Mockito.verify(monitorService1, Mockito.timeout(1000)).fetchAllClusters(BeaconSystem.XPIPE_ONE_WAY.getSystemName());
        Mockito.verify(monitorService0, Mockito.timeout(1000)).fetchAllClusters(BeaconSystem.XPIPE_BI_DIRECTION.getSystemName());
        Mockito.verify(monitorService1, Mockito.timeout(1000)).fetchAllClusters(BeaconSystem.XPIPE_BI_DIRECTION.getSystemName());
        Mockito.verify(monitorService0, Mockito.never()).unregisterCluster(anyString(), anyString());
        Mockito.verify(alertManager).alert("", "", null, ALERT_TYPE.TOO_MANY_CLUSTERS_EXCLUDE_FROM_BEACON, oneWayNeedExcludeClusters.toString());
        Mockito.verify(alertManager).alert("", "", null, ALERT_TYPE.TOO_MANY_CLUSTERS_EXCLUDE_FROM_BEACON, biNeedExcludeClusters.toString());
    }

    @Test
    public void testClusterActiveDcZoneNotSupport() throws NoSuchFieldException, IllegalAccessException {
        Mockito.when(config.getBeaconSupportZones()).thenReturn(Collections.singleton("FRA"));
        Mockito.when(monitorService0.fetchAllClusters(BeaconSystem.XPIPE_ONE_WAY.getSystemName())).thenReturn(Sets.newHashSet("cluster2","hetero-cluster2"));
        Mockito.when(monitorService1.fetchAllClusters(BeaconSystem.XPIPE_ONE_WAY.getSystemName())).thenReturn(Sets.newHashSet("cluster1","hetero-cluster1"));
        Mockito.when(monitorService0.fetchAllClusters(BeaconSystem.XPIPE_BI_DIRECTION.getSystemName())).thenReturn(Sets.newHashSet("bi-cluster2"));
        Mockito.when(monitorService1.fetchAllClusters(BeaconSystem.XPIPE_BI_DIRECTION.getSystemName())).thenReturn(Sets.newHashSet("bi-cluster1"));

        Map<BeaconSystem, Map<Long, Set<String>>> mockmap = mockClustersByBeaconSystemOrg(getOrgServicesMap(), metaCache, config);
        Mockito.when(monitorManager.clustersByBeaconSystemOrg()).thenReturn(mockmap);

        check.doAction();

        Mockito.verify(monitorService0, Mockito.timeout(1000)).fetchAllClusters(BeaconSystem.XPIPE_ONE_WAY.getSystemName());
        Mockito.verify(monitorService1, Mockito.timeout(1000)).fetchAllClusters(BeaconSystem.XPIPE_ONE_WAY.getSystemName());
        Mockito.verify(monitorService0, Mockito.timeout(1000)).fetchAllClusters(BeaconSystem.XPIPE_BI_DIRECTION.getSystemName());
        Mockito.verify(monitorService1, Mockito.timeout(1000)).fetchAllClusters(BeaconSystem.XPIPE_BI_DIRECTION.getSystemName());
        Mockito.verify(monitorService0, Mockito.timeout(1000)).unregisterCluster(BeaconSystem.XPIPE_ONE_WAY.getSystemName(), "cluster2");
        Mockito.verify(monitorService1, Mockito.timeout(1000)).unregisterCluster(BeaconSystem.XPIPE_ONE_WAY.getSystemName(), "cluster1");
        Mockito.verify(monitorService0, Mockito.timeout(1000)).unregisterCluster(BeaconSystem.XPIPE_ONE_WAY.getSystemName(), "hetero-cluster2");
        Mockito.verify(monitorService1, Mockito.timeout(1000)).unregisterCluster(BeaconSystem.XPIPE_ONE_WAY.getSystemName(), "hetero-cluster1");
        Mockito.verify(monitorService0, Mockito.timeout(1000)).unregisterCluster(BeaconSystem.XPIPE_BI_DIRECTION.getSystemName(), "bi-cluster2");
        Mockito.verify(monitorService1, Mockito.timeout(1000)).unregisterCluster(BeaconSystem.XPIPE_BI_DIRECTION.getSystemName(), "bi-cluster1");
    }

    @Test
    public void testUnregisterActiveDcNotSupported() throws NoSuchFieldException, IllegalAccessException {
        XpipeMeta xpipeMeta = new XpipeMeta();
        xpipeMeta.addDc(new DcMeta("jq").setZone("SHA").addCluster(new ClusterMeta("cluster1").setOrgId(1).setType(ClusterType.ONE_WAY.name()).setActiveDc("fra")))
                .addDc(new DcMeta("fra").setZone("FRA").addCluster(new ClusterMeta("cluster1").setOrgId(1).setType(ClusterType.ONE_WAY.name()).setActiveDc("fra")));
        Mockito.when(metaCache.getXpipeMeta()).thenReturn(xpipeMeta);
        Mockito.when(config.getBeaconSupportZones()).thenReturn(Collections.singleton("SHA"));

        Map<BeaconSystem, Map<Long, Set<String>>> mockmap = mockClustersByBeaconSystemOrg(getOrgServicesMap(), metaCache, config);
        Mockito.when(monitorManager.clustersByBeaconSystemOrg()).thenReturn(mockmap);

        Mockito.when(monitorService1.fetchAllClusters(BeaconSystem.XPIPE_ONE_WAY.getSystemName())).thenReturn(Sets.newHashSet("cluster1"));
        check.doAction();

        Mockito.verify(monitorService1, Mockito.timeout(1000)).fetchAllClusters(BeaconSystem.XPIPE_ONE_WAY.getSystemName());
        Mockito.verify(monitorService1, Mockito.timeout(1000)).unregisterCluster(BeaconSystem.XPIPE_ONE_WAY.getSystemName(), "cluster1");
    }

    private XpipeMeta mockXPipeMeta() {
        XpipeMeta xpipeMeta = new XpipeMeta();
        xpipeMeta.addDc(new DcMeta("jq").setZone("SHA")
                .addCluster(new ClusterMeta("cluster1").setOrgId(1).setType(ClusterType.ONE_WAY.name()).setActiveDc("jq"))
                .addCluster(new ClusterMeta("cluster2").setOrgId(2).setType(ClusterType.ONE_WAY.name()).setActiveDc("jq"))
                .addCluster(new ClusterMeta("bi-cluster1").setOrgId(1).setType(ClusterType.BI_DIRECTION.name()))
                .addCluster(new ClusterMeta("bi-cluster2").setOrgId(2).setType(ClusterType.BI_DIRECTION.name()))
                .addCluster(new ClusterMeta("hetero-cluster1").setOrgId(1).setType(ClusterType.HETERO.name()).setAzGroupType(ClusterType.ONE_WAY.name()).setActiveDc("jq"))
                .addCluster(new ClusterMeta("hetero-cluster2").setOrgId(2).setType(ClusterType.HETERO.name()).setAzGroupType(ClusterType.ONE_WAY.name()).setActiveDc("jq"))
        );

        return xpipeMeta;
    }

    private Map<BeaconSystem, Map<Long, Set<String>>> mockClustersByBeaconSystemOrg(Map<Long, List<MonitorService>> orgServicesMap, MetaCache meta, ConsoleCommonConfig consoleCommonConfig) throws NoSuchFieldException, IllegalAccessException {
        Map<Long, DefaultMonitorClusterManager> clusterManagers = new HashMap<>();
        for(Map.Entry<Long, List<MonitorService>> entry : orgServicesMap.entrySet()) {
            clusterManagers.put(entry.getKey(), new DefaultMonitorClusterManager(meta, 1, entry.getValue(), 1));
        }

        DefaultMonitorManager monitorManager1 = new DefaultMonitorManager(meta, consoleConfig, consoleCommonConfig);
        Field field = DefaultMonitorManager.class.getDeclaredField("orgMonitorMap");
        field.setAccessible(true);
        field.set(monitorManager1, clusterManagers);
        return monitorManager1.clustersByBeaconSystemOrg();
    }

}
