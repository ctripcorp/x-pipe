package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.redis.checker.AbstractCheckerIntegrationTest;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckActionListener;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckInstanceManager;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.ping.PingAction;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.ping.PingActionContext;
import com.ctrip.xpipe.simpleserver.Server;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * @author chen.zhu
 * <p>
 * Oct 10, 2018
 */
public class DelayPingActionCollectorTest extends AbstractCheckerIntegrationTest {

    @Autowired
    private DefaultDelayPingActionCollector collector;

    @Autowired
    private List<HealthCheckActionListener> listeners;

    @Autowired
    private HealthCheckInstanceManager instanceManager;

    private RedisHealthCheckInstance instance;

    private Server server;

    @Before
    public void beforeDelayPingActionCollectorTest () throws Exception {
        server = startServerWithFlexibleResult(new Callable<String>() {

            private int count = 0;

            @Override
            public String call() throws Exception {
                if(count ++ > 5) {
                    return null;
                }
                return "+PONG\r\n";
            }
        });
        instance = instanceManager.getOrCreate(newRandomFakeRedisMeta().setPort(server.getPort()));
    }

    @After
    public void afterDelayPingActionCollectorTest () throws Exception {
        if(server != null) {
            server.stop();
        }
    }

    @Test
    public void testHealthState() {

    }

    @Ignore
    @Test
    public void testPingAction() throws Exception {
        PingAction action = new PingAction(scheduled, instance, executors);
        action.addListeners(listeners);
        LifecycleHelper.initializeIfPossible(action);
        LifecycleHelper.startIfPossible(action);
        while(!Thread.currentThread().isInterrupted()) {
            sleep(1000);
            HostPort hostPort = action.getActionInstance().getCheckInfo().getHostPort();
            HEALTH_STATE health_state = collector.createHealthStatus(instance).getState();
            logger.info("[{}]", health_state);
            logger.info("[Health State][{}] {}", hostPort, collector.getState(hostPort));
        }
    }

    @Ignore
    @Test
    public void testSpring() throws Exception {
        HealthCheckActionListener listener1 = listeners.get(3);
        HealthCheckActionListener listener2 = listeners.get(4);
        logger.info("{}", listener1);
        logger.info("{}", listener2);
    }

    @Ignore
    @Test
    public void testReflection() throws Exception {
        Class clazz = Class.forName(DefaultDelayPingActionCollector.class.getName() + "$PingActionListener");
        logger.info("{}", clazz.getName());
        Constructor[] constructors = clazz.getDeclaredConstructors();
        Constructor constructor = constructors[0];
        HealthCheckActionListener listener = (HealthCheckActionListener) constructor.newInstance(collector);
        logger.info("{}", listener.worksfor(new PingActionContext(null, true)));
    }

}