package com.ctrip.xpipe.redis.console.healthcheck.actions.delay;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckActionListener;
import com.ctrip.xpipe.redis.console.healthcheck.actions.ping.PingAction;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author chen.zhu
 * <p>
 * Sep 06, 2018
 */
public class DefaultDelayServiceTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private List<HealthCheckActionListener> listeners;

//    @Test(expected = NullPointerException.class)
//    public void testOnAction() {
//        service.onAction(new DelayActionContext(null, System.currentTimeMillis()));
//    }

    @Test(expected = NullPointerException.class)
    public void testOnActionWithPing() {
        PingAction action = new PingAction(scheduled, null, executors);
//        interaction.
    }

    @Test
    public void testDelayNotZero() {
        long nano = DelayAction.SAMPLE_LOST_AND_NO_PONG;
        logger.info("{}", TimeUnit.NANOSECONDS.toMillis(nano));
    }
}