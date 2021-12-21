package com.ctrip.xpipe.redis.checker.healthcheck.actions.ping;

import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import org.junit.Test;

import static org.mockito.Mockito.mock;

/**
 * @author Slight
 * <p>
 * Dec 07, 2021 11:55 PM
 */
public class PingActionContextTest {

    @Test
    public void fixNPE() {
        PingActionContext context;
        context = new PingActionContext(mock(RedisHealthCheckInstance.class), false, new Exception());
        if (context.getResult()) {

        }
        context = new PingActionContext(mock(RedisHealthCheckInstance.class), false);
        if (context.getResult()) {

        }
        context = new PingActionContext(mock(RedisHealthCheckInstance.class), true);
        if (context.getResult()) {

        }

    }
}