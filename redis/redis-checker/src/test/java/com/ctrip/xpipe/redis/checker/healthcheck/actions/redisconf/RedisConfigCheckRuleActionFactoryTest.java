package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisconf;

import com.ctrip.xpipe.redis.checker.AbstractCheckerIntegrationTest;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class RedisConfigCheckRuleActionFactoryTest extends AbstractCheckerIntegrationTest {

    @Autowired
    private RedisConfigCheckRuleActionFactory factory;

    private RedisHealthCheckInstance instance;

    @Before
    public void testRedisConfigCheckRuleActionFactoryTest() throws Exception {
        List<RedisCheckRule> redisCheckRules = new LinkedList<>();

        Map<String, String> param1 = new HashMap<>();
        param1.put("configCheckName", "repl-backlog-size");
        param1.put("expectedVaule", "256");

        Map<String, String> param2 = new HashMap<>();
        param2.put("configCheckName", "repl-backlog-size");
        param2.put("expectedVaule", "256");

        redisCheckRules.add(new RedisCheckRule("config", param1));
        redisCheckRules.add(new RedisCheckRule("config", param2));
        redisCheckRules.add(new RedisCheckRule("info", param1));

        instance = newRandomRedisHealthCheckInstance(randomPort(), redisCheckRules);
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
