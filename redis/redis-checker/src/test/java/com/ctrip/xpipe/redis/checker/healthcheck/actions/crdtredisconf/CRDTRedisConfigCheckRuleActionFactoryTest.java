package com.ctrip.xpipe.redis.checker.healthcheck.actions.crdtredisconf;

import com.ctrip.xpipe.redis.checker.AbstractCheckerIntegrationTest;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisconf.RedisCheckRule;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


public class CRDTRedisConfigCheckRuleActionFactoryTest extends AbstractCheckerIntegrationTest {

    @Autowired
    private CRDTRedisConfigCheckRuleActionFactory factory;

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

        instance = newRandomBiDirectionRedisHealthCheckInstance(randomPort(), redisCheckRules);
        instance.register(factory.create(instance));
    }

    @Test
    public void testCreate(){
        Assert.assertTrue(instance.getHealthCheckActions().size() > 0);
        CRDTRedisConfigCheckRuleAction action = (CRDTRedisConfigCheckRuleAction) instance.getHealthCheckActions().get(0);
        Assert.assertTrue(action.getLifecycleState().isEmpty());
    }

    @Test
    public void testSuppot(){
        Assert.assertEquals(CRDTRedisConfigCheckRuleAction.class, factory.support());
    }
}
