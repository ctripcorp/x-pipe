package com.ctrip.xpipe.redis.console.healthcheck.actions.delay;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.console.healthcheck.impl.DefaultRedisInstanceInfo;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class MultiMasterDelayActionControllerTest {

    @Mock
    private RedisHealthCheckInstance instance;

    private MultiMasterDelayActionController controller = new MultiMasterDelayActionController();

    private String currentDc = FoundationService.DEFAULT.getDataCenter();
    private String remoteDc = "remote-dc";

    @Test
    public void testShouldCheck() {
        Assert.assertTrue(controller.shouldCheck(mockInstance(currentDc, true)));
        Assert.assertTrue(controller.shouldCheck(mockInstance(currentDc, false)));
        Assert.assertTrue(controller.shouldCheck(mockInstance(remoteDc, true)));
        Assert.assertFalse(controller.shouldCheck(mockInstance(remoteDc, false)));
    }

    private RedisHealthCheckInstance mockInstance(String dcId, boolean isMaster) {
        RedisInstanceInfo info = new DefaultRedisInstanceInfo(dcId, "cluster", "shard", new HostPort(), null, ClusterType.BI_DIRECTION);
        info.isMaster(isMaster);
        Mockito.when(instance.getRedisInstanceInfo()).thenReturn(info);
        return instance;
    }

}
