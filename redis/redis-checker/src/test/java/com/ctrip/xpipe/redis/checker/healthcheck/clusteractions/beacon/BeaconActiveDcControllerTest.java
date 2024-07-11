package com.ctrip.xpipe.redis.checker.healthcheck.clusteractions.beacon;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.healthcheck.ClusterHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.ClusterInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultClusterInstanceInfo;
import com.ctrip.xpipe.redis.core.config.ConsoleCommonConfig;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * @author lishanglin
 * date 2021/1/19
 */
@RunWith(MockitoJUnitRunner.class)
public class BeaconActiveDcControllerTest extends AbstractCheckerTest {

    private BeaconActiveDcController controller;

    private ClusterHealthCheckInstance instance;

    private ClusterInstanceInfo info;

    @Mock
    private ConsoleCommonConfig consoleCommonConfig;

    @Mock
    private MetaCache metaCache;

    @Before
    public void setupBeaconActiveDcControllerTest() {
        controller = new BeaconActiveDcController(consoleCommonConfig, metaCache);
        instance = Mockito.mock(ClusterHealthCheckInstance.class);
        info = new DefaultClusterInstanceInfo("cluster1", "jq", ClusterType.ONE_WAY, 1);

        Mockito.when(instance.getCheckInfo()).thenReturn(info);
        Mockito.when(consoleCommonConfig.getBeaconSupportZone()).thenReturn("SHA");
        Mockito.when(metaCache.isDcInRegion(Mockito.anyString(), Mockito.anyString())).thenReturn(true);
    }

    @Test
    public void checkFromActiveDc() {
        Assert.assertTrue(controller.shouldCheck(instance));
    }

    @Test
    public void checkFromBackupDc() {
        info.setActiveDc("backup");
        Assert.assertFalse(controller.shouldCheck(instance));
    }

    @Test
    public void activeDcZoneNotSupported() {
        Mockito.when(metaCache.isDcInRegion(Mockito.anyString(), Mockito.anyString())).thenReturn(false);
        Assert.assertFalse(controller.shouldCheck(instance));
    }

}
