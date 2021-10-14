package com.ctrip.xpipe.redis.checker.healthcheck.actions.delay;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultRedisInstanceInfo;
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

    private MultiMasterDelayActionController controller = new MultiMasterDelayActionController(FoundationService.DEFAULT);

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
        Mockito.when(instance.getCheckInfo()).thenReturn(info);
        return instance;
    }

}
