package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisconf;

import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultRedisInstanceInfo;
import com.ctrip.xpipe.simpleserver.Server;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.function.Function;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.never;

public class RedisConfigCheckRuleActionTest extends AbstractCheckerTest {

    private RedisConfigCheckRuleAction action;

    private AlertManager alertManager;

    private Server redis;


    @Before
    public void beforeRedisConfigCheckRuleActionTest() throws Exception {
        alertManager = mock(AlertManager.class);
        redis = startServer("*2\r\n$17\r\nrepl-backlog-size\r\n$3\r\n256\r\n");

        List<RedisCheckRule> redisCheckRules = new LinkedList<>();
        Map<String, String> param1 = new HashMap<>();
        param1.put("configCheckName", "repl-backlog-size");
        param1.put("expectedVaule", "128");
        redisCheckRules.add(new RedisCheckRule("config", param1));
        RedisHealthCheckInstance instance = newRandomRedisHealthCheckInstance(redis.getPort(), redisCheckRules);

        action = new RedisConfigCheckRuleAction(scheduled, instance, executors, alertManager, redisCheckRules);
    }

    @Test
    public void testDoTask() {
        doNothing().when(alertManager).alert(any(DefaultRedisInstanceInfo.class), any(ALERT_TYPE.class), anyString());
        action.doTask();
        sleep(1000);

        verify(alertManager, times(1)).alert(any(DefaultRedisInstanceInfo.class), any(ALERT_TYPE.class), anyString());
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
