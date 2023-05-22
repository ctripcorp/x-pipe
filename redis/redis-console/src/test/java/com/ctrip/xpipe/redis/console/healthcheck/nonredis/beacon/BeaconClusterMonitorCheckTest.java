package com.ctrip.xpipe.redis.console.healthcheck.nonredis.beacon;

import com.ctrip.xpipe.api.migration.auto.MonitorService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.migration.auto.BeaconSystem;
import com.ctrip.xpipe.redis.console.migration.auto.MonitorServiceManager;
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

import java.util.HashMap;
import java.util.Set;

/**
 * @author lishanglin
 * date 2021/1/19
 */
@RunWith(MockitoJUnitRunner.class)
public class BeaconClusterMonitorCheckTest extends AbstractConsoleTest {

    @InjectMocks
    private BeaconClusterMonitorCheck check;
    
    @Mock
    private MonitorServiceManager monitorServiceManager;

    @Mock
    private MetaCache metaCache;

    @Mock
    private AlertManager alertManager;

    @Mock
    private MonitorService monitorService0;

    @Mock
    private MonitorService monitorService1;

    @Mock
    private ConsoleConfig config;


    @Before
    public void setupBeaconClusterMonitorCheckTest() {
        Mockito.when(monitorServiceManager.getAllServices())
                .thenReturn(new HashMap<Long, MonitorService>() {{
                    put(0L, monitorService0);
                    put(1L, monitorService1);
                }});
        Mockito.when(metaCache.getXpipeMeta()).thenReturn(mockXPipeMeta());
    }

    @Test
    public void testDoCheck() {
        check.doAction();
        sleep(1000);
        Mockito.verify(monitorService0).fetchAllClusters(Mockito.anyString());
        Mockito.verify(monitorService1).fetchAllClusters(Mockito.anyString());
    }

    @Test
    public void testTooManyClusterNeedExcluded() {
        Mockito.when(config.monitorUnregisterProtectCount()).thenReturn(1);
        Mockito.when(config.getMigrationUnsupportedClusters()).thenReturn(Sets.newHashSet("cluster1","cluster2","cluster3","clusterx"));

        Set<String> needExcludeClusters = Sets.newHashSet("clusterx", "clustery");
        Mockito.when(monitorService0.fetchAllClusters(Mockito.anyString())).thenReturn(Sets.newHashSet("clusterx", "clustery"));

        check.doAction();
        sleep(1000);
        Mockito.verify(monitorService0).fetchAllClusters(Mockito.anyString());
        Mockito.verify(monitorService1).fetchAllClusters(Mockito.anyString());
        Mockito.verify(monitorService0, Mockito.never()).unregisterCluster(Mockito.anyString(), Mockito.anyString());
        Mockito.verify(alertManager).alert("", "", null, ALERT_TYPE.TOO_MANY_CLUSTERS_EXCLUDE_FROM_BEACON, needExcludeClusters.toString());
    }

    @Test
    public void testUnsupportedMigrationClusters(){
        Mockito.when(config.monitorUnregisterProtectCount()).thenReturn(10);
        Mockito.when(config.getMigrationUnsupportedClusters()).thenReturn(Sets.newHashSet("cluster1","cluster3"));
        Mockito.when(monitorService0.fetchAllClusters(Mockito.anyString())).thenReturn(Sets.newHashSet("cluster1", "cluster2"));

        check.doAction();
        sleep(1000);
        Mockito.verify(monitorService0).fetchAllClusters(Mockito.anyString());
        Mockito.verify(monitorService1).fetchAllClusters(Mockito.anyString());
        Mockito.verify(monitorService0, Mockito.times(1)).unregisterCluster(BeaconSystem.getDefault().getSystemName(), "cluster1");
    }

    private XpipeMeta mockXPipeMeta() {
        XpipeMeta xpipeMeta = new XpipeMeta();
        xpipeMeta.addDc(new DcMeta("jq")
                .addCluster(new ClusterMeta("cluster1").setOrgId(1).setType(ClusterType.ONE_WAY.name()))
                .addCluster(new ClusterMeta("cluster2").setOrgId(2).setType(ClusterType.ONE_WAY.name()))
        );

        return xpipeMeta;
    }

}
