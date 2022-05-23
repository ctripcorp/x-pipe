package com.ctrip.xpipe.redis.checker.healthcheck.clusteractions.beacon;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.api.migration.auto.MonitorService;
import com.ctrip.xpipe.redis.checker.BeaconManager;
import com.ctrip.xpipe.api.migration.auto.data.MonitorGroupMeta;
import com.ctrip.xpipe.redis.checker.healthcheck.ClusterHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.ClusterInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultClusterInstanceInfo;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.Set;

/**
 * @author lishanglin
 * date 2021/1/19
 */
@RunWith(MockitoJUnitRunner.class)
public class BeaconMetaCheckActionTest extends AbstractCheckerTest {

    private BeaconMetaCheckAction action;

    @Mock
    private BeaconManager beaconManager;

    @Mock
    private MonitorService monitorService;

    @Mock
    private ClusterHealthCheckInstance instance;

    private ClusterInstanceInfo info;

    private Set<MonitorGroupMeta> groups;

    int orgId = 1;

    @Before
    public void setupBeaconMetaCheckActionTest() {
        action = new BeaconMetaCheckAction(scheduled, instance, executors, beaconManager);

        String cluster = "cluster1";
        info = new DefaultClusterInstanceInfo(cluster, "jq", ClusterType.ONE_WAY, orgId);
        groups = Sets.newHashSet(new MonitorGroupMeta("shard1", "jq", Collections.singleton(new HostPort("127.0.0.1", 6379)), true));

        Mockito.when(instance.getCheckInfo()).thenReturn(info);
    }

    @Test
    public void testDoTask() {
        action.doTask();
        Mockito.verify(beaconManager).registerCluster(info.getClusterId(), ClusterType.ONE_WAY, orgId);
    }

}
