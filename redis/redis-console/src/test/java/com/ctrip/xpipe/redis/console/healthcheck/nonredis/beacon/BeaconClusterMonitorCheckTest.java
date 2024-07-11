package com.ctrip.xpipe.redis.console.healthcheck.nonredis.beacon;

import com.ctrip.xpipe.api.migration.auto.MonitorService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.migration.auto.BeaconSystem;
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


    @Before
    public void setupBeaconClusterMonitorCheckTest() {
        Map<Long, List<MonitorService>> orgServicesMap = new HashMap<>();
        orgServicesMap.put(0L, Collections.singletonList(monitorService0));
        orgServicesMap.put(1L, Collections.singletonList(monitorService1));
        Mockito.when(monitorManager.getAllServices()).thenReturn(orgServicesMap);
        Mockito.when(metaCache.getXpipeMeta()).thenReturn(mockXPipeMeta());
        Mockito.when(config.getBeaconSupportZone()).thenReturn("");
        Mockito.when(config.monitorUnregisterProtectCount()).thenReturn(10);
        Mockito.when(metaCache.isDcInRegion(anyString(), anyString())).thenReturn(true);
    }

    @Test
    public void testDoCheck() {
        Mockito.when(monitorService0.fetchAllClusters(anyString())).thenReturn(Sets.newHashSet("cluster2"));
        Mockito.when(monitorService1.fetchAllClusters(anyString())).thenReturn(Sets.newHashSet("cluster1"));
        check.doAction();

        Mockito.verify(monitorService0, Mockito.timeout(1000)).fetchAllClusters(anyString());
        Mockito.verify(monitorService1, Mockito.timeout(1000)).fetchAllClusters(anyString());
        Mockito.verify(monitorService0, Mockito.never()).unregisterCluster(anyString(), anyString());
        Mockito.verify(monitorService1, Mockito.never()).unregisterCluster(anyString(), anyString());
    }

    @Test
    public void testTooManyClusterNeedExcluded() {
        Mockito.when(config.monitorUnregisterProtectCount()).thenReturn(1);

        Set<String> needExcludeClusters = Sets.newHashSet("clusterx", "clustery");
        Mockito.when(monitorService0.fetchAllClusters(anyString())).thenReturn(Sets.newHashSet("clusterx", "clustery"));

        check.doAction();
        Mockito.verify(monitorService0, Mockito.timeout(1000)).fetchAllClusters(anyString());
        Mockito.verify(monitorService1, Mockito.timeout(1000)).fetchAllClusters(anyString());
        Mockito.verify(monitorService0, Mockito.never()).unregisterCluster(anyString(), anyString());
        Mockito.verify(alertManager).alert("", "", null, ALERT_TYPE.TOO_MANY_CLUSTERS_EXCLUDE_FROM_BEACON, needExcludeClusters.toString());
    }

    @Test
    public void testClusterActiveDcZoneNotSupport() {
        Mockito.when(config.getBeaconSupportZone()).thenReturn("SHA");
        Mockito.when(metaCache.isDcInRegion(anyString(), anyString())).thenReturn(false);
        Mockito.when(monitorService0.fetchAllClusters(anyString())).thenReturn(Sets.newHashSet("cluster2"));
        Mockito.when(monitorService1.fetchAllClusters(anyString())).thenReturn(Sets.newHashSet("cluster1"));
        check.doAction();

        Mockito.verify(monitorService0, Mockito.timeout(1000)).fetchAllClusters(anyString());
        Mockito.verify(monitorService1, Mockito.timeout(1000)).fetchAllClusters(anyString());
        Mockito.verify(monitorService0, Mockito.timeout(1000)).unregisterCluster(BeaconSystem.XPIPE_ONE_WAY.getSystemName(), "cluster2");
        Mockito.verify(monitorService1, Mockito.timeout(1000)).unregisterCluster(BeaconSystem.XPIPE_ONE_WAY.getSystemName(), "cluster1");
    }

    private XpipeMeta mockXPipeMeta() {
        XpipeMeta xpipeMeta = new XpipeMeta();
        xpipeMeta.addDc(new DcMeta("jq")
                .addCluster(new ClusterMeta("cluster1").setOrgId(1).setType(ClusterType.ONE_WAY.name()).setActiveDc("jq"))
                .addCluster(new ClusterMeta("cluster2").setOrgId(2).setType(ClusterType.ONE_WAY.name()).setActiveDc("jq"))
        );

        return xpipeMeta;
    }

}
