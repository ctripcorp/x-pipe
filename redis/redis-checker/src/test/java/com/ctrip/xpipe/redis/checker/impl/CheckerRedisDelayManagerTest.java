package com.ctrip.xpipe.redis.checker.impl;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.delay.DelayActionContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;

/**
 * @author lishanglin
 * date 2021/3/17
 */
public class CheckerRedisDelayManagerTest extends AbstractCheckerTest {

    private CheckerRedisDelayManager manager;

    private RedisHealthCheckInstance instance;

    private HealthCheckAction action;

    @Before
    public void setupCheckerRedisDelayManagerTest() throws Exception {
        manager = new CheckerRedisDelayManager();
        instance = newRandomRedisHealthCheckInstance(6379);
        action = Mockito.mock(HealthCheckAction.class);
        Mockito.when(action.getActionInstance()).thenReturn(instance);
    }

    @Test
    public void testOnAction() {
        manager.onAction(new DelayActionContext(instance, 100L));
        Assert.assertEquals(Collections.singletonMap(new HostPort("127.0.0.1", 6379), 100L), manager.getAllDelays());
    }

    @Test
    public void testStopWatch() {
        manager.onAction(new DelayActionContext(instance, 100L));
        manager.stopWatch(action);
        Assert.assertEquals(Collections.emptyMap(), manager.getAllDelays());

    }

}
