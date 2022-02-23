package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisconf;

import com.ctrip.xpipe.redis.checker.AbstractCheckerIntegrationTest;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.LinkedList;
import java.util.List;

public class RedisConfigCheckRuleActionFactoryTest extends AbstractCheckerIntegrationTest {

    @Autowired
    private RedisConfigCheckRuleActionFactory factory;

    private RedisHealthCheckInstance instance;

    @Before
    public void testRedisConfigCheckRuleActionFactoryTest() throws Exception {
        List<RedisConfigCheckRule> redisConfigCheckRules = new LinkedList<>();
        redisConfigCheckRules.add(new RedisConfigCheckRule("config", "repl_backlog_size", "256"));
        redisConfigCheckRules.add(new RedisConfigCheckRule("config", "repl_backlog_size", "128"));
        redisConfigCheckRules.add(new RedisConfigCheckRule("info", "repl_backlog_size", "256"));

        instance = newRandomRedisHealthCheckInstance(randomPort(), redisConfigCheckRules);
        instance.register(factory.create(instance));
    }

    @Test
    public void testCreate(){
        Assert.assertTrue(instance.getHealthCheckActions().size() > 0);
        RedisConfigCheckRuleAction action = (RedisConfigCheckRuleAction) instance.getHealthCheckActions().get(0);
        Assert.assertTrue(action.getLifecycleState().isEmpty());
    }

    @Test
    public void testSuppot(){
        Assert.assertEquals(RedisConfigCheckRuleAction.class, factory.support());
    }


}
