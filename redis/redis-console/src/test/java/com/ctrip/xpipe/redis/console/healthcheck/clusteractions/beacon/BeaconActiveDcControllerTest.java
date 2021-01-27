package com.ctrip.xpipe.redis.console.healthcheck.clusteractions.beacon;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.healthcheck.ClusterHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.ClusterInstanceInfo;
import com.ctrip.xpipe.redis.console.healthcheck.impl.DefaultClusterInstanceInfo;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * @author lishanglin
 * date 2021/1/19
 */
public class BeaconActiveDcControllerTest extends AbstractConsoleTest {

    private BeaconActiveDcController controller;

    private ClusterHealthCheckInstance instance;

    private ClusterInstanceInfo info;

    @Before
    public void setupBeaconActiveDcControllerTest() {
        controller = new BeaconActiveDcController();
        instance = Mockito.mock(ClusterHealthCheckInstance.class);
        info = new DefaultClusterInstanceInfo("cluster1", "jq", ClusterType.ONE_WAY, 1);

        Mockito.when(instance.getCheckInfo()).thenReturn(info);
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

}
