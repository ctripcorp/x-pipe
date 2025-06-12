package com.ctrip.xpipe.redis.checker.healthcheck.actions.crdtredisconf;

import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisconf.RedisCheckRule;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisconf.RedisConfigCheckRuleActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisconf.RedisConfigCheckRuleActionListener;
import com.ctrip.xpipe.simpleserver.Server;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class CRDTRedisConfigCheckRuleActionTest extends AbstractCheckerTest {

    private CRDTRedisConfigCheckRuleAction action;

    private AlertManager alertManager;

    private RedisConfigCheckRuleActionContext actionContext = null;

    @Mock
    private RedisConfigCheckRuleActionListener listener;

    private Server redis;

    @Before
    public void startCRDTRedisConfigCheckRuleActionTest() throws Exception {
        MockitoAnnotations.initMocks(this);
        redis = startServer(randomPort(), new Function<String, String>() {
            @Override
            public String apply(String s) {
                if (s.trim().toLowerCase().startsWith("config crdt.get")) {
                    return "*2\r\n$17\r\nrepl-backlog-size\r\n$3\r\n256\r\n";
                } else {
                    return "+OK\r\n";
                }
            }
        });

        alertManager = mock(AlertManager.class);
        Map<String, String> param = new HashMap<>();
        param.put("configCheckName", "repl-backlog-size");
        param.put("expectedVaule", "128");

        List<RedisCheckRule> redisCheckRules = new LinkedList<>();
        redisCheckRules.add(new RedisCheckRule("config", param));
        RedisHealthCheckInstance instance = newRandomBiDirectionRedisHealthCheckInstance(redis.getPort(), redisCheckRules);

        action = new CRDTRedisConfigCheckRuleAction(scheduled, instance, executors, redisCheckRules);
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
