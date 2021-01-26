package com.ctrip.xpipe.redis.console.healthcheck.clusteractions.beacon;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.api.migration.auto.MonitorService;
import com.ctrip.xpipe.redis.console.migration.auto.MonitorServiceManager;
import com.ctrip.xpipe.api.migration.auto.data.MonitorGroupMeta;
import com.ctrip.xpipe.redis.console.healthcheck.ClusterHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.ClusterInstanceInfo;
import com.ctrip.xpipe.redis.console.healthcheck.impl.DefaultClusterInstanceInfo;
import com.ctrip.xpipe.redis.console.service.meta.BeaconMetaService;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Collections;
import java.util.Set;

/**
 * @author lishanglin
 * date 2021/1/19
 */
@RunWith(MockitoJUnitRunner.class)
public class BeaconMetaCheckActionTest extends AbstractConsoleTest {

    private BeaconMetaCheckAction action;

    @Mock
    private BeaconMetaService beaconMetaService;

    @Mock
    private MonitorServiceManager monitorServiceManager;

    @Mock
    private MonitorService monitorService;

    @Mock
    private ClusterHealthCheckInstance instance;

    private ClusterInstanceInfo info;

    private Set<MonitorGroupMeta> groups;

    @Before
    public void setupBeaconMetaCheckActionTest() {
        action = new BeaconMetaCheckAction(scheduled, instance, executors, beaconMetaService, monitorServiceManager);

        int orgId = 1;
        String cluster = "cluster1";
        info = new DefaultClusterInstanceInfo(cluster, "jq", ClusterType.ONE_WAY, orgId);
        groups = Sets.newHashSet(new MonitorGroupMeta("shard1", "jq", Collections.singleton(new HostPort("127.0.0.1", 6379)), true));

        Mockito.when(monitorServiceManager.getOrCreate(orgId)).thenReturn(monitorService);
        Mockito.when(instance.getCheckInfo()).thenReturn(info);
        Mockito.when(beaconMetaService.buildBeaconGroups(cluster)).thenReturn(groups);
    }

    @Test
    public void testDoTask() {
        action.doTask();
        Mockito.verify(monitorService).registerCluster(info.getClusterId(), groups);
    }

}
