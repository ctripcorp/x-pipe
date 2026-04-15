package com.ctrip.xpipe.redis.checker.healthcheck.clusteractions.beacon;

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

@RunWith(MockitoJUnitRunner.class)
public class BeaconConsistencyCheckActionTest extends AbstractCheckerTest {

    private BeaconConsistencyCheckAction action;

    @Mock
    private ClusterHealthCheckInstance instance;

    @Mock
    private BeaconManager beaconManager;

    private ClusterInstanceInfo info;

    String cluster = "cluster1";

    private int orgId = 1;

    private String lastModifyTime = "20200101103030001";

    @Before
    public void setupBeaconMetaCheckActionTest() {
        action = new BeaconConsistencyCheckAction(scheduled, instance, executors, beaconManager);
        info = new DefaultClusterInstanceInfo(cluster, "jq", ClusterType.ONE_WAY, orgId, lastModifyTime);
        Mockito.when(instance.getCheckInfo()).thenReturn(info);
    }

    @Test
    public void beaconNoCluster_doRegister() {
        Mockito.when(beaconManager.checkClusterHash(cluster, ClusterType.ONE_WAY, orgId, lastModifyTime, BeaconRouteType.DR))
                .thenReturn(BeaconCheckStatus.CLUSTER_NOT_FOUND);
        action.doTask();
        Mockito.verify(beaconManager).registerCluster(info.getClusterId(), ClusterType.ONE_WAY, orgId, lastModifyTime, BeaconRouteType.DR);
    }

    @Test
    public void clusterChanged_doUpdateMeta() {
        Mockito.when(beaconManager.checkClusterHash(cluster, ClusterType.ONE_WAY, orgId, lastModifyTime, BeaconRouteType.DR))
                .thenReturn(BeaconCheckStatus.INCONSISTENCY);

        action.doTask();

        Mockito.verify(beaconManager).checkClusterHash(cluster, ClusterType.ONE_WAY, orgId, lastModifyTime, BeaconRouteType.DR);
        Mockito.verify(beaconManager).registerCluster(info.getClusterId(), ClusterType.ONE_WAY, orgId, lastModifyTime, BeaconRouteType.DR);
    }

    @Test
    public void clusterChanged_butBeaconLastModifyTimeNewer_ignoreUpdate() {
        Mockito.when(beaconManager.checkClusterHash(cluster, ClusterType.ONE_WAY, orgId, lastModifyTime, BeaconRouteType.DR))
                .thenReturn(BeaconCheckStatus.INCONSISTENCY_IGNORE);

        action.doTask();

        Mockito.verify(beaconManager).checkClusterHash(cluster, ClusterType.ONE_WAY, orgId, lastModifyTime, BeaconRouteType.DR);
        Mockito.verify(beaconManager, Mockito.never()).registerCluster(info.getClusterId(), ClusterType.ONE_WAY, orgId, lastModifyTime, BeaconRouteType.DR);
    }

}
