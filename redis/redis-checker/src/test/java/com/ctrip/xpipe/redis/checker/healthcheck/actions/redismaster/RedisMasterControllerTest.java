package com.ctrip.xpipe.redis.checker.healthcheck.actions.redismaster;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import org.junit.Assert;
import org.junit.Test;

public class RedisMasterControllerTest extends AbstractCheckerTest {

    private String currentDc = FoundationService.DEFAULT.getDataCenter();

    @Test
    public void testCurrentDcRedisMasterController() throws Exception {
        CurrentDcRedisMasterController currentDcRedisMasterController = new CurrentDcRedisMasterController(FoundationService.DEFAULT);
        Assert.assertTrue(currentDcRedisMasterController.shouldCheck(
                newRandomRedisHealthCheckInstance(FoundationService.DEFAULT.getDataCenter(), ClusterType.BI_DIRECTION, 6379)));
        Assert.assertFalse(currentDcRedisMasterController.shouldCheck(newRandomRedisHealthCheckInstance("remoteDc", ClusterType.BI_DIRECTION, 6379)));
    }

    @Test
    public void testActiveDcRedisMasterController() throws Exception {
        ActiveDcRedisMasterController activeDcRedisMasterController = new ActiveDcRedisMasterController();
        Assert.assertTrue(activeDcRedisMasterController.shouldCheck(newRandomRedisHealthCheckInstance(currentDc, currentDc, 6379)));
        Assert.assertFalse(activeDcRedisMasterController.shouldCheck(newRandomRedisHealthCheckInstance(currentDc, "activeDc", 6379)));
    }

}
