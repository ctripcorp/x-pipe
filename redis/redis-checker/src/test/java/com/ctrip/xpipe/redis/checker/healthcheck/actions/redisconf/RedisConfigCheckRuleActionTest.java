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

import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
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

        List<RedisConfigCheckRule> redisConfigCheckRules = new LinkedList<>();
        redisConfigCheckRules.add(new RedisConfigCheckRule("config", "repl-backlog-size", "128"));
        RedisHealthCheckInstance instance = newRandomRedisHealthCheckInstance(redis.getPort(), redisConfigCheckRules);

        action = new RedisConfigCheckRuleAction(scheduled, instance, executors, alertManager, redisConfigCheckRules);
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
