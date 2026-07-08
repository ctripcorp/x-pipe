package com.ctrip.xpipe.redis.checker.healthcheck.clusteractions.beacon;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.BeaconManager;
import com.ctrip.xpipe.redis.checker.BeaconRouteType;
import com.ctrip.xpipe.redis.checker.healthcheck.ClusterHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.ClusterInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultClusterInstanceInfo;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;

@RunWith(MockitoJUnitRunner.class)
public class SentinelBeaconConsistencyCheckActionTest extends AbstractCheckerTest {

    private SentinelBeaconConsistencyCheckAction action;

    @Mock
    private ClusterHealthCheckInstance instance;

    @Mock
    private BeaconManager beaconManager;

    private ClusterInstanceInfo info;

    private final String cluster = "cluster1";

    private final String dc = "jq";

    private final int orgId = 1;

    private final String lastModifyTime = "20200101103030001";

    @Before
    public void setupSentinelBeaconConsistencyCheckActionTest() {
        action = new SentinelBeaconConsistencyCheckAction(scheduled, instance, executors, beaconManager);
        info = new DefaultClusterInstanceInfo(cluster, dc, ClusterType.SINGLE_DC, orgId, lastModifyTime);
        Mockito.when(instance.getCheckInfo()).thenReturn(info);
    }

    @Test
    public void sentinelClusterNotFound_shouldRegister() {
        String currentDc = FoundationService.DEFAULT.getDataCenter();
        Mockito.when(beaconManager.checkClusterHash(cluster, currentDc, ClusterType.SINGLE_DC, orgId, lastModifyTime,
                BeaconRouteType.SENTINEL)).thenReturn(BeaconCheckStatus.CLUSTER_NOT_FOUND);

        action.doTask();

        Mockito.verify(beaconManager).registerCluster(cluster, currentDc, ClusterType.SINGLE_DC, orgId, lastModifyTime,
                BeaconRouteType.SENTINEL, Collections.emptyMap());
    }

    @Test
    public void sentinelMetaInconsistent_shouldRegister() {
        String currentDc = FoundationService.DEFAULT.getDataCenter();
        Mockito.when(beaconManager.checkClusterHash(cluster, currentDc, ClusterType.SINGLE_DC, orgId, lastModifyTime,
                BeaconRouteType.SENTINEL)).thenReturn(BeaconCheckStatus.INCONSISTENCY);

        action.doTask();

        Mockito.verify(beaconManager).checkClusterHash(cluster, currentDc, ClusterType.SINGLE_DC, orgId, lastModifyTime,
                BeaconRouteType.SENTINEL);
        Mockito.verify(beaconManager).registerCluster(cluster, currentDc, ClusterType.SINGLE_DC, orgId, lastModifyTime,
                BeaconRouteType.SENTINEL, Collections.emptyMap());
    }

    @Test
    public void sentinelMetaConsistent_shouldNotRegister() {
        String currentDc = FoundationService.DEFAULT.getDataCenter();
        Mockito.when(beaconManager.checkClusterHash(cluster, currentDc, ClusterType.SINGLE_DC, orgId, lastModifyTime,
                BeaconRouteType.SENTINEL)).thenReturn(BeaconCheckStatus.CONSISTENCY);

        action.doTask();

        Mockito.verify(beaconManager).checkClusterHash(cluster, currentDc, ClusterType.SINGLE_DC, orgId, lastModifyTime,
                BeaconRouteType.SENTINEL);
        Mockito.verify(beaconManager, Mockito.never()).registerCluster(Mockito.anyString(), Mockito.anyString(),
                Mockito.any(), Mockito.anyInt(), Mockito.anyString(), Mockito.eq(BeaconRouteType.SENTINEL),
                Mockito.anyMap());
    }

    @Test
    public void sentinelInconsistentIgnore_shouldNotRegister() {
        String currentDc = FoundationService.DEFAULT.getDataCenter();
        Mockito.when(beaconManager.checkClusterHash(cluster, currentDc, ClusterType.SINGLE_DC, orgId, lastModifyTime,
                BeaconRouteType.SENTINEL)).thenReturn(BeaconCheckStatus.INCONSISTENCY_IGNORE);

        action.doTask();

        Mockito.verify(beaconManager, Mockito.never()).registerCluster(Mockito.anyString(), Mockito.anyString(),
                Mockito.any(), Mockito.anyInt(), Mockito.anyString(), Mockito.eq(BeaconRouteType.SENTINEL),
                Mockito.anyMap());
    }
}
