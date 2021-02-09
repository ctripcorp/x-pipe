package com.ctrip.xpipe.redis.console.healthcheck.actions.redisstats.backstreaming;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.healthcheck.BiDirectionSupport;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckActionController;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author lishanglin
 * date 2021/1/26
 */
public class BackStreamingActionFactoryTest extends AbstractConsoleIntegrationTest {

    @Autowired
    public BackStreamingActionFactory factory;

    @Test
    public void testCreate() throws Exception {
        RedisHealthCheckInstance instance = newRandomRedisHealthCheckInstance(FoundationService.DEFAULT.getDataCenter(), ClusterType.BI_DIRECTION, 6379);
        BackStreamingAction action = factory.create(instance);
        action.doStart();

        Assert.assertTrue(factory.support().isInstance(action));
        Assert.assertEquals(1, action.getControllers().size());
        Assert.assertTrue((action.getControllers().get(0)).shouldCheck(action.getActionInstance()));
        Assert.assertTrue(action.getListeners().stream().allMatch(listener -> listener instanceof BiDirectionSupport));
    }

}
