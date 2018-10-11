package com.ctrip.xpipe.redis.console.healthcheck.actions.interaction;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckActionListener;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Sep 06, 2018
 */
public class DelayPingActionListenerTest extends AbstractConsoleIntegrationTest {

//    @Autowired
//    private DelayPingActionCollector listener;

    @Autowired
    private List<HealthCheckActionListener> listeners;

//    @Test(expected = NullPointerException.class)
//    public void testOnDelayAction() {
//        listener.onAction(new DelayActionContext(null, System.currentTimeMillis()));
//    }
//
//    @Test(expected = NullPointerException.class)
//    public void testOnPingAction() {
//        listener.onAction(new PingActionContext(null, true));
//    }

    @Test
    public void testListeners() {
        for(HealthCheckActionListener listener : listeners) {
            logger.info("[listener] {}", listener.getClass().getName());
        }
    }

}