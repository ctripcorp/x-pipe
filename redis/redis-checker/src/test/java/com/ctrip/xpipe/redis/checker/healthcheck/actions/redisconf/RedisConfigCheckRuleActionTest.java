package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisconf;

import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.checker.healthcheck.ActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckActionListener;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultRedisInstanceInfo;
import com.ctrip.xpipe.simpleserver.Server;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.*;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import static com.ctrip.xpipe.redis.checker.healthcheck.actions.redisconf.AbstractRedisConfigRuleAction.CONFIG_CHECK_NAME;
import static com.ctrip.xpipe.redis.checker.healthcheck.actions.redisconf.AbstractRedisConfigRuleAction.EXPECTED_VAULE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.never;

public class RedisConfigCheckRuleActionTest extends AbstractCheckerTest {

    private RedisConfigCheckRuleAction action;

    private RedisConfigCheckRuleActionContext actionContext = null;

    @Mock
    private RedisConfigCheckRuleActionListener listener;

    private AlertManager alertManager;

    private Server redis;


    @Before
    public void beforeRedisConfigCheckRuleActionTest() throws Exception {
        MockitoAnnotations.initMocks(this);

        alertManager = mock(AlertManager.class);
        redis = startServer("*2\r\n$17\r\nrepl-backlog-size\r\n$3\r\n256\r\n");

        List<RedisCheckRule> redisCheckRules = new LinkedList<>();
        Map<String, String> param1 = new HashMap<>();
        param1.put("configCheckName", "repl-backlog-size");
        param1.put("expectedVaule", "128");
        redisCheckRules.add(new RedisCheckRule("config", param1));
        RedisHealthCheckInstance instance = newRandomRedisHealthCheckInstance(redis.getPort(), redisCheckRules);

        action = new RedisConfigCheckRuleAction(scheduled, instance, executors, redisCheckRules);
        action.addListener(listener);

        Mockito.when(listener.worksfor(Mockito.any())).thenReturn(true);
        doNothing().when(listener).onAction(Mockito.any());
        Mockito.doAnswer(invocation -> {
            actionContext = invocation.getArgument(0, RedisConfigCheckRuleActionContext.class);
            return null;
        }).when(listener).onAction(Mockito.any());

    }

    @Test
    public void testDoTask() throws TimeoutException {
        action.doTask();
        waitConditionUntilTimeOut(() -> null != actionContext, 1000);
        Assert.assertEquals("256", actionContext.getResult());
    }

    @After
    public void stopRedis() {
        if(redis != null) {
            try {
                redis.stop();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}
