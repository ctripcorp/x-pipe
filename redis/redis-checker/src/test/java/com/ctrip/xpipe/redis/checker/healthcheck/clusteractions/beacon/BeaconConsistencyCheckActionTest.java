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
public class BeaconConsistencyCheckActionTest extends AbstractCheckerTest {

    private BeaconConsistencyCheckAction action;

    @Mock
    private ClusterHealthCheckInstance instance;

    @Mock
    private BeaconManager beaconManager;

    @Mock
    private StabilityHolder stabilityHolder;

    private ClusterInstanceInfo info;

    String cluster = "cluster1";

    private String dc = "jq";

    private int orgId = 1;

    private String lastModifyTime = "20200101103030001";

    @Before
    public void setupBeaconMetaCheckActionTest() {
        action = new BeaconConsistencyCheckAction(scheduled, instance, executors, beaconManager, stabilityHolder);
        info = new DefaultClusterInstanceInfo(cluster, dc, ClusterType.ONE_WAY, orgId, lastModifyTime);
        Mockito.when(instance.getCheckInfo()).thenReturn(info);
        Mockito.when(stabilityHolder.isSiteStable()).thenReturn(true);
    }

    @Test
    public void beaconNoCluster_doRegister() {
        String currentDc = FoundationService.DEFAULT.getDataCenter();
        Mockito.when(beaconManager.checkClusterHash(cluster, currentDc, ClusterType.ONE_WAY, orgId, BeaconRouteType.DR))
                .thenReturn(BeaconCheckStatus.CLUSTER_NOT_FOUND);
        action.doTask();
        Mockito.verify(beaconManager).registerCluster(info.getClusterId(), currentDc, ClusterType.ONE_WAY, orgId,
                BeaconRouteType.DR, Collections.emptyMap());
    }

    @Test
    public void clusterChanged_doUpdateMeta() {
        String currentDc = FoundationService.DEFAULT.getDataCenter();
        Mockito.when(beaconManager.checkClusterHash(cluster, currentDc, ClusterType.ONE_WAY, orgId, BeaconRouteType.DR))
                .thenReturn(BeaconCheckStatus.INCONSISTENCY);

        action.doTask();

        Mockito.verify(beaconManager).checkClusterHash(cluster, currentDc, ClusterType.ONE_WAY, orgId, BeaconRouteType.DR);
        Mockito.verify(beaconManager).registerCluster(info.getClusterId(), currentDc, ClusterType.ONE_WAY, orgId,
                BeaconRouteType.DR, Collections.emptyMap());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void clusterChanged_butLocalIsolated_skipRegister() {
        String currentDc = FoundationService.DEFAULT.getDataCenter();
        Mockito.when(stabilityHolder.isSiteStable()).thenReturn(false);
        Mockito.when(beaconManager.checkClusterHash(cluster, currentDc, ClusterType.ONE_WAY, orgId, BeaconRouteType.DR))
                .thenReturn(BeaconCheckStatus.INCONSISTENCY);

        action.doTask();

        Mockito.verify(beaconManager, Mockito.never()).registerCluster(Mockito.anyString(), Mockito.anyString(),
                Mockito.any(ClusterType.class), Mockito.anyInt(), Mockito.eq(BeaconRouteType.DR), Mockito.anyMap());
    }

}
