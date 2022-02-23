package com.ctrip.xpipe.redis.checker.healthcheck.actions.crdtredisconf;

import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisconf.RedisConfigCheckRule;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultRedisInstanceInfo;
import com.ctrip.xpipe.simpleserver.Server;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

public class CRDTRedisConfigCheckRuleActionTest extends AbstractCheckerTest {

    private CRDTRedisConfigCheckRuleAction action;

    private AlertManager alertManager;

    private Server redis;

    @Before
    public void startCRDTRedisConfigCheckRuleActionTest() throws Exception {
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
        List<RedisConfigCheckRule> redisConfigCheckRules = new LinkedList<>();
        redisConfigCheckRules.add(new RedisConfigCheckRule("config", "repl-backlog-size", "128"));
        RedisHealthCheckInstance instance = newRandomBiDirectionRedisHealthCheckInstance(redis.getPort(), redisConfigCheckRules);

        action = new CRDTRedisConfigCheckRuleAction(scheduled, instance, executors, alertManager, redisConfigCheckRules);
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