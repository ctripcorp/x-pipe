package com.ctrip.xpipe.redis.console.beacon;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.migration.auto.MonitorService;
import com.ctrip.xpipe.redis.console.config.model.BeaconClusterRoute;
import com.ctrip.xpipe.redis.console.config.model.BeaconOrgRoute;
import com.ctrip.xpipe.redis.console.migration.auto.DefaultMonitorManager;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.ctrip.xpipe.redis.console.constant.XPipeConsoleConstant.DEFAULT_ORG_ID;

/**
 * @author lishanglin
 * date 2021/1/18
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultMonitorManagerTest extends AbstractTest {

    @Mock
    private MetaCache metaCache;
    @Mock
    private ConsoleConfig config;

    private DefaultMonitorManager beaconServiceManager;

    private final String defaultBeaconHost1 = "http://127.0.0.1:8080";
    private final String defaultBeaconHost2 = "http://127.0.0.2:8080";
    private final String beaconHost1 = "http://10.0.0.1:8080";
    private final String beaconHost2 = "http://10.0.0.2:8080";
    private final BeaconClusterRoute clusterRoute1 = new BeaconClusterRoute("beacon-1", defaultBeaconHost1, 100);
    private final BeaconClusterRoute clusterRoute2 = new BeaconClusterRoute("beacon-2", defaultBeaconHost2, 80);
    private final BeaconClusterRoute clusterRoute3 = new BeaconClusterRoute("beacon-3", beaconHost1, 100);
    private final BeaconClusterRoute clusterRoute4 = new BeaconClusterRoute("beacon-4", beaconHost2, 100);
    private final BeaconOrgRoute orgRoute1 = new BeaconOrgRoute(0L, Lists.newArrayList(clusterRoute1, clusterRoute2), 100);
    private final BeaconOrgRoute orgRoute2 = new BeaconOrgRoute(1L, Collections.singletonList(clusterRoute3), 100);
    private final BeaconOrgRoute orgRoute3 = new BeaconOrgRoute(2L, Collections.singletonList(clusterRoute4), 100);

    private final Map<Long, String> beacons = new HashMap<Long, String>() {{
        put(1L, beaconHost1);
        put(2L, beaconHost2);
    }};

    @Before
    public void setupDefaultBeaconServiceManagerTest() {
        Mockito.when(config.getBeaconOrgRoutes()).thenReturn(Lists.newArrayList(orgRoute1, orgRoute2, orgRoute3));
        Mockito.when(config.getClusterHealthCheckInterval()).thenReturn(1000);
        beaconServiceManager = new DefaultMonitorManager(metaCache, config);
    }

    @Test
    public void testGetHostByOrg() {
        MonitorService monitorService1 = beaconServiceManager.get(1L, "cluster2");
        Assert.assertEquals(beacons.get(1L), monitorService1.getHost());
        MonitorService monitorService2 = beaconServiceManager.get(2L, "cluster3");
        Assert.assertEquals(beacons.get(2L), monitorService2.getHost());
    }

    @Test
    public void testGetHostByUnknownOrg() {
        Set<String> expectedHosts = new HashSet<>();
        expectedHosts.add(defaultBeaconHost1);
        expectedHosts.add(defaultBeaconHost2);

        MonitorService monitorService1 = beaconServiceManager.get(3L, "cluster");
        MonitorService monitorService2 = beaconServiceManager.get(4L, "cluster-xxx");
        MonitorService monitorService3 = beaconServiceManager.get(5L, "use this to try");
        MonitorService monitorService4 = beaconServiceManager.get(6L, "god is abc");
        Set<String> actualHosts = Stream.of(monitorService1, monitorService2, monitorService3, monitorService4)
            .map(MonitorService::getHost)
            .collect(Collectors.toSet());

        Assert.assertEquals(expectedHosts, actualHosts);
    }

    @Test
    public void testGetAllBeacons() {
        Map<Long, List<MonitorService>> beaconServices = beaconServiceManager.getAllServices();
        Assert.assertEquals(3, beaconServices.size());
        Assert.assertEquals(defaultBeaconHost1, beaconServices.get(DEFAULT_ORG_ID).get(0).getHost());
        Assert.assertEquals(defaultBeaconHost2, beaconServices.get(DEFAULT_ORG_ID).get(1).getHost());
        Assert.assertEquals(beacons.get(1L), beaconServices.get(1L).get(0).getHost());
        Assert.assertEquals(beacons.get(2L), beaconServices.get(2L).get(0).getHost());
    }

}
