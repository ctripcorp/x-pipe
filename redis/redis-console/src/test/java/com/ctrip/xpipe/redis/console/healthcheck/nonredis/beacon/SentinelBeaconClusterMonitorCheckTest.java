package com.ctrip.xpipe.redis.console.healthcheck.nonredis.beacon;

import com.ctrip.xpipe.api.migration.auto.MonitorService;
import com.ctrip.xpipe.redis.checker.BeaconRouteType;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.migration.auto.MonitorManager;
import com.ctrip.xpipe.redis.core.beacon.BeaconSystem;
import com.google.common.collect.Sets;
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

@RunWith(MockitoJUnitRunner.class)
public class SentinelBeaconClusterMonitorCheckTest extends AbstractConsoleTest {

    @InjectMocks
    private SentinelBeaconClusterMonitorCheck check;

    @Mock
    private MonitorManager monitorManager;

    @Mock
    private AlertManager alertManager;

    @Mock
    private MonitorService monitorService;

    @Test
    public void shouldUseSentinelRouteAndCleanupRedundantClusters() {
        Map<Long, List<MonitorService>> services = new HashMap<>();
        services.put(1L, Collections.singletonList(monitorService));
        Mockito.when(monitorManager.getAllServices(BeaconRouteType.SENTINEL)).thenReturn(services);

        Map<Long, Set<String>> expectedByOrg = new HashMap<>();
        expectedByOrg.put(1L, Collections.singleton("cluster-a"));
        Map<BeaconSystem, Map<Long, Set<String>>> bySystem = new HashMap<>();
        bySystem.put(BeaconSystem.XPIPE_ONE_WAY, expectedByOrg);
        Mockito.when(monitorManager.clustersByBeaconSystemOrg(BeaconRouteType.SENTINEL)).thenReturn(bySystem);

        Mockito.when(monitorService.fetchAllClusters(BeaconSystem.XPIPE_ONE_WAY.getSystemName()))
                .thenReturn(Sets.newHashSet("cluster-a", "cluster-b"));

        check.doAction();

        Mockito.verify(monitorService, Mockito.timeout(1000))
                .fetchAllClusters(BeaconSystem.XPIPE_ONE_WAY.getSystemName());
        Mockito.verify(monitorService, Mockito.timeout(1000))
                .unregisterCluster(BeaconSystem.XPIPE_ONE_WAY.getSystemName(), "cluster-b");
    }
}
