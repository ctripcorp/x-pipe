package com.ctrip.xpipe.redis.checker.healthcheck.actions.keeper.infoStats;

import com.ctrip.xpipe.redis.checker.AbstractCheckerIntegrationTest;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.KeeperHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.leader.SiteLeaderAwareHealthCheckAction;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Created by yu
 * 2023/8/28
 */
public class KeeperInfoStatsActionFactoryTest extends AbstractCheckerIntegrationTest {

    @Autowired
    private KeeperInfoStatsActionFactory factory;

    private KeeperHealthCheckInstance instance;

    @Before
    public void init() throws Exception {
        instance = newRandomKeeperHealthCheckInstance(randomString(), randomPort());
        instance.register(factory.create(instance));
    }

    @Test
    public void testCreate() {
        Assert.assertTrue(instance.getHealthCheckActions().size() > 0);
        KeeperInfoStatsAction action = (KeeperInfoStatsAction) instance.getHealthCheckActions().get(0);
        Assert.assertTrue(action.getLifecycleState().isEmpty());
    }

    @Test
    public void testSupport() {
        Assert.assertEquals(KeeperInfoStatsAction.class, factory.support());
    }

    @Test
    public void testDestroy() {
        HealthCheckAction target = null;
        for(HealthCheckAction action : instance.getHealthCheckActions()) {
            if(action.getClass().isAssignableFrom(factory.support())) {
                target = action;
                factory.destroy((SiteLeaderAwareHealthCheckAction) target);
            }
        }


        logger.info("{}", instance.getHealthCheckActions());
        instance.unregister(target);
        instance.unregister(target);
        Assert.assertTrue(instance.getHealthCheckActions().isEmpty());
    }

}