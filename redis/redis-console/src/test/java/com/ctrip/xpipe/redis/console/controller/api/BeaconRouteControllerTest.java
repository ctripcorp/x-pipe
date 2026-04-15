package com.ctrip.xpipe.redis.console.controller.api;

import com.ctrip.xpipe.api.migration.auto.MonitorService;
import com.ctrip.xpipe.redis.checker.BeaconRouteType;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.migration.auto.MonitorManager;
import com.ctrip.xpipe.redis.core.beacon.BeaconSystem;
import org.junit.Assert;
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
public class BeaconRouteControllerTest extends AbstractConsoleTest {

    @InjectMocks
    private BeaconRouteController controller;

    @Mock
    private MonitorManager monitorManager;

    @Mock
    private MonitorService monitorService;

    @Test
    public void shouldReturnSentinelRouteForClusterAndOrg() {
        Mockito.when(monitorManager.get(1L, "cluster-a", BeaconRouteType.SENTINEL)).thenReturn(monitorService);
        Mockito.when(monitorService.getName()).thenReturn("beacon-a");
        Mockito.when(monitorService.getHost()).thenReturn("http://beacon-a");

        Map<String, String> result = controller.getSentinelBeaconRoute("cluster-a", 1L, null);

        Assert.assertEquals("beacon-a", result.get("beaconName"));
        Assert.assertEquals("http://beacon-a", result.get("beaconHost"));
        Assert.assertEquals("SENTINEL", result.get("routeType"));
    }

    @Test
    public void shouldReturnManagedClustersByBeacon() {
        Map<Long, List<MonitorService>> services = new HashMap<>();
        services.put(1L, Collections.singletonList(monitorService));
        Mockito.when(monitorManager.getAllServices(BeaconRouteType.SENTINEL)).thenReturn(services);
        Mockito.when(monitorService.getName()).thenReturn("beacon-a");
        Mockito.when(monitorService.getHost()).thenReturn("http://beacon-a");
        Mockito.when(monitorService.fetchAllClusters(BeaconSystem.XPIPE_ONE_WAY.getSystemName()))
                .thenReturn(Collections.singleton("cluster-a"));

        Map<String, Set<String>> result = controller.getSentinelBeaconClusters(null, null);

        Assert.assertEquals(Collections.singleton("cluster-a"), result.get("beacon-a@http://beacon-a"));
    }
}
