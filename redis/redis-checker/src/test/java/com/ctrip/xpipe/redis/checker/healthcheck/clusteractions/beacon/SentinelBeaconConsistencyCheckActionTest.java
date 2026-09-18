package com.ctrip.xpipe.redis.checker.healthcheck.clusteractions.beacon;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.BeaconManager;
import com.ctrip.xpipe.redis.checker.healthcheck.ClusterHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.ClusterInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultClusterInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.stability.StabilityHolder;
import com.ctrip.xpipe.redis.core.beacon.BeaconRouteType;
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

    @Mock
    private StabilityHolder stabilityHolder;

    private ClusterInstanceInfo info;

    private final String cluster = "cluster1";

    private final String dc = "jq";

    private final int orgId = 1;

    private final String lastModifyTime = "20200101103030001";

    @Before
    public void setupSentinelBeaconConsistencyCheckActionTest() {
        action = new SentinelBeaconConsistencyCheckAction(scheduled, instance, executors, beaconManager, stabilityHolder);
        info = new DefaultClusterInstanceInfo(cluster, dc, ClusterType.SINGLE_DC, orgId, lastModifyTime);
        Mockito.when(instance.getCheckInfo()).thenReturn(info);
        Mockito.when(stabilityHolder.isSiteStable()).thenReturn(true);
    }

    @Test
    public void sentinelClusterNotFound_shouldRegister() {
        String currentDc = FoundationService.DEFAULT.getDataCenter();
        Mockito.when(beaconManager.checkClusterHash(cluster, currentDc, ClusterType.SINGLE_DC, orgId,
                BeaconRouteType.SENTINEL)).thenReturn(BeaconCheckStatus.CLUSTER_NOT_FOUND);

        action.doTask();

        Mockito.verify(beaconManager).registerCluster(cluster, currentDc, ClusterType.SINGLE_DC, orgId,
                BeaconRouteType.SENTINEL, Collections.emptyMap());
    }

    @Test
    public void sentinelMetaInconsistent_shouldRegister() {
        String currentDc = FoundationService.DEFAULT.getDataCenter();
        Mockito.when(beaconManager.checkClusterHash(cluster, currentDc, ClusterType.SINGLE_DC, orgId,
                BeaconRouteType.SENTINEL)).thenReturn(BeaconCheckStatus.INCONSISTENCY);

        action.doTask();

        Mockito.verify(beaconManager).checkClusterHash(cluster, currentDc, ClusterType.SINGLE_DC, orgId,
                BeaconRouteType.SENTINEL);
        Mockito.verify(beaconManager).registerCluster(cluster, currentDc, ClusterType.SINGLE_DC, orgId,
                BeaconRouteType.SENTINEL, Collections.emptyMap());
    }

    @Test
    public void sentinelMetaConsistent_shouldNotRegister() {
        String currentDc = FoundationService.DEFAULT.getDataCenter();
        Mockito.when(beaconManager.checkClusterHash(cluster, currentDc, ClusterType.SINGLE_DC, orgId,
                BeaconRouteType.SENTINEL)).thenReturn(BeaconCheckStatus.CONSISTENCY);

        action.doTask();

        Mockito.verify(beaconManager, Mockito.never()).registerCluster(Mockito.anyString(), Mockito.anyString(),
                Mockito.any(), Mockito.anyInt(), Mockito.eq(BeaconRouteType.SENTINEL),
                Mockito.anyMap());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void sentinelMetaInconsistent_butLocalIsolated_skipRegister() {
        String currentDc = FoundationService.DEFAULT.getDataCenter();
        Mockito.when(stabilityHolder.isSiteStable()).thenReturn(false);
        Mockito.when(beaconManager.checkClusterHash(cluster, currentDc, ClusterType.SINGLE_DC, orgId,
                BeaconRouteType.SENTINEL)).thenReturn(BeaconCheckStatus.INCONSISTENCY);

        action.doTask();

        Mockito.verify(beaconManager, Mockito.never()).registerCluster(Mockito.anyString(), Mockito.anyString(),
                Mockito.any(ClusterType.class), Mockito.anyInt(), Mockito.eq(BeaconRouteType.SENTINEL),
                Mockito.anyMap());
    }
}
