package com.ctrip.xpipe.redis.console.beacon;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.migration.auto.MonitorService;
import com.ctrip.xpipe.redis.console.migration.auto.DefaultMonitorServiceManager;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;

import static com.ctrip.xpipe.redis.console.constant.XPipeConsoleConstant.DEFAULT_ORG_ID;

/**
 * @author lishanglin
 * date 2021/1/18
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultMonitorServiceManagerTest extends AbstractTest {

    @Mock
    private ConsoleConfig config;

    private DefaultMonitorServiceManager beaconServiceManager;

    private String defaultBeaconHost = "http://127.0.0.1:8080";

    private Map<Long, String> beacons = new HashMap<Long, String>() {{
        put(1L, "http://10.0.1:8080");
        put(2L, "http://10.0.2:8080");
    }};

    @Before
    public void setupDefaultBeaconServiceManagerTest() {
        beaconServiceManager = new DefaultMonitorServiceManager(config);
        Mockito.when(config.getDefaultBeaconHost()).thenReturn(defaultBeaconHost);
        Mockito.when(config.getBeaconHosts()).thenReturn(beacons);
    }

    @Test
    public void testGetHostByOrg() {
        MonitorService monitorService = beaconServiceManager.getOrCreate(2);
        Assert.assertEquals(beacons.get(2L), monitorService.getHost());
    }

    @Test
    public void testGetHostByUnknownOrg() {
        MonitorService monitorService = beaconServiceManager.getOrCreate(3);
        Assert.assertEquals(defaultBeaconHost, monitorService.getHost());
    }

    @Test
    public void testGetAllBeacons() {
        Map<Long, MonitorService> beaconServices = beaconServiceManager.getAllServices();
        Assert.assertEquals(3, beaconServices.size());
        Assert.assertEquals(defaultBeaconHost, beaconServices.get(DEFAULT_ORG_ID).getHost());
        Assert.assertEquals(beacons.get(1L), beaconServices.get(1L).getHost());
        Assert.assertEquals(beacons.get(2L), beaconServices.get(2L).getHost());
    }

}
