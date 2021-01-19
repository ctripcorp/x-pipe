package com.ctrip.xpipe.redis.console.healthcheck.nonredis.beacon;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.alert.AlertManager;
import com.ctrip.xpipe.redis.console.beacon.BeaconService;
import com.ctrip.xpipe.redis.console.beacon.BeaconServiceManager;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

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
    private BeaconServiceManager beaconServiceManager;

    @Mock
    private MetaCache metaCache;

    @Mock
    private AlertManager alertManager;

    @Mock
    private BeaconService beaconService0;

    @Mock
    private BeaconService beaconService1;


    @Before
    public void setupBeaconClusterMonitorCheckTest() {
        Mockito.when(beaconServiceManager.getAllServices())
                .thenReturn(new HashMap<Long, BeaconService>() {{
                    put(0L, beaconService0);
                    put(1L, beaconService1);
                }});
        Mockito.when(metaCache.getXpipeMeta()).thenReturn(mockXPipeMeta());
    }

    @Test
    public void testDoCheck() {
        check.doCheck();
        sleep(1000);
        Mockito.verify(beaconService0).fetchAllClusters();
        Mockito.verify(beaconService1).fetchAllClusters();
    }

    @Test
    public void testTooManyClusterNeedExcluded() {
        check.MONITOR_UNREGISTER_PROTECT_COUNT = 1;
        Set<String> needExcludeClusters = Sets.newHashSet("clusterx", "clustery");
        Mockito.when(beaconService0.fetchAllClusters()).thenReturn(Sets.newHashSet("clusterx", "clustery"));

        check.doCheck();
        sleep(1000);
        Mockito.verify(beaconService0).fetchAllClusters();
        Mockito.verify(beaconService1).fetchAllClusters();
        Mockito.verify(beaconService0, Mockito.never()).unregisterCluster(Mockito.anyString());
        Mockito.verify(alertManager).alert("", "", null, ALERT_TYPE.TOO_MANY_CLUSTERS_EXCLUDE_FROM_BEACON, needExcludeClusters.toString());
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
