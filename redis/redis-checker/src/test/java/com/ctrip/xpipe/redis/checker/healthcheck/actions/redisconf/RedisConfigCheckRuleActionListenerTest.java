package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisconf;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultRedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultRedisInstanceInfo;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class RedisConfigCheckRuleActionListenerTest {

    RedisConfigCheckRuleActionListener listener;

    private AlertManager alertManager;

    @Before
    public void before() {
        alertManager = mock(AlertManager.class);
        listener = new RedisConfigCheckRuleActionListener(alertManager);
    }

    @Test
    public void testOnAction() {
        doNothing().when(alertManager).alert(any(DefaultRedisInstanceInfo.class), any(ALERT_TYPE.class), anyString());
        DefaultRedisHealthCheckInstance instance = new DefaultRedisHealthCheckInstance();
        Map<String, String> param1 = new HashMap<>();
        param1.put("configCheckName", "repl-backlog-size");
        param1.put("expectedVaule", "128");
        RedisCheckRule redisCheckRule = new RedisCheckRule("config", param1);
        instance.setInstanceInfo(new DefaultRedisInstanceInfo("jq", "cluster1", "cluster1_1", new HostPort("1.1.1.1", 6379), "jq", ClusterType.ONE_WAY, Collections.emptyList()));
        RedisConfigCheckRuleActionContext actionContext = new RedisConfigCheckRuleActionContext(instance,"256", redisCheckRule);

        listener.onAction(actionContext);
        verify(alertManager, times(1)).alert(any(), any(), any());
    }


}