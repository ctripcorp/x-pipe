package com.ctrip.xpipe.redis.checker.healthcheck.actions.gtidgap;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class GtidGapCheckActionControllerTest extends AbstractCheckerTest {


    @InjectMocks
    private DefaultGtidGapCheckActionController controller;

    @Mock
    private FoundationService foundationService;

    @Test
    public void shouldCheckTest() throws Exception {
        when(foundationService.getDataCenter()).thenReturn("NT");

        controller = new DefaultGtidGapCheckActionController(foundationService);
        RedisHealthCheckInstance redisHealthCheckInstance = newRandomRedisHealthCheckInstance("NT", "JQ", 6379);
        redisHealthCheckInstance.getCheckInfo().setAzGroupType(null);
        Assert.assertFalse(controller.shouldCheck(redisHealthCheckInstance));

        redisHealthCheckInstance.getCheckInfo().setAzGroupType(ClusterType.ONE_WAY.name());
        Assert.assertFalse(controller.shouldCheck(redisHealthCheckInstance));

        redisHealthCheckInstance.getCheckInfo().setAzGroupType(ClusterType.SINGLE_DC.name());
        Assert.assertTrue(controller.shouldCheck(redisHealthCheckInstance));

        when(foundationService.getDataCenter()).thenReturn("NT2");
        controller = new DefaultGtidGapCheckActionController(foundationService);
        Assert.assertFalse(controller.shouldCheck(redisHealthCheckInstance));
    }

}
