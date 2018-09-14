package com.ctrip.xpipe.redis.console.healthcheck.action;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.healthcheck.delay.DelayActionContext;
import com.ctrip.xpipe.redis.console.healthcheck.ping.PingActionContext;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author chen.zhu
 * <p>
 * Sep 06, 2018
 */
public class DelayPingActionListenerTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private DelayPingActionListener listener;

    @Test(expected = NullPointerException.class)
    public void testOnDelayAction() {
        listener.onAction(new DelayActionContext(null, System.currentTimeMillis()));
    }

    @Test(expected = NullPointerException.class)
    public void testOnPingAction() {
        listener.onAction(new PingActionContext(null, true));
    }
}