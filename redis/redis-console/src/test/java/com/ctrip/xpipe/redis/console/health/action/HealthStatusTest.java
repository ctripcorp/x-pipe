package com.ctrip.xpipe.redis.console.health.action;

import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author wenchao.meng
 *         <p>
 *         May 04, 2017
 */
public class HealthStatusTest extends AbstractConsoleTest {

    private HostPort hostPort;
    private int downAfterMilli = 200;
    private int healthDelayMilli = 100;

    @Before
    public void beforeHealthStatusTest() {
        this.hostPort = new HostPort("127.0.0.1", randomPort());
    }

    @Test
    public void testDown() {

        AtomicBoolean isDown = new AtomicBoolean();

        HealthStatus healthStatus = new HealthStatus(hostPort, () -> downAfterMilli, () -> healthDelayMilli, scheduled);
        healthStatus.addObserver(new Observer() {
            @Override
            public void update(Object args, Observable observable) {
                logger.debug("{}, {}", args, observable);
                if (args instanceof InstanceUp) {
                    isDown.set(false);
                } else if (args instanceof InstanceDown) {
                    isDown.set(true);
                } else {
                    throw new IllegalStateException("unknown " + args);
                }
            }
        });
        healthStatus.delay(healthDelayMilli);
        Assert.assertFalse(isDown.get());

        sleep(downAfterMilli * 3);
        Assert.assertTrue(isDown.get());

        healthStatus.delay(healthDelayMilli);
        Assert.assertFalse(isDown.get());
    }

    @Test
    public void testSetUp() {

        int count = 20000;
        List<HealthStatus> allHealth = new LinkedList<>();

        long begin = System.nanoTime();

        for (int i = 0; i < count; i++) {
            HealthStatus healthStatus = new HealthStatus(hostPort, () -> 10000, () -> healthDelayMilli, scheduled);
            healthStatus.delay(1);
        }

        long end = System.nanoTime();
        logger.info("{} ms", (end - begin) / 1000000);
    }

    @Test
    public void testToString() {

        HealthStatus healthStatus = new HealthStatus(hostPort, () -> downAfterMilli, () -> healthDelayMilli, scheduled);

        long begin = System.currentTimeMillis();

        String result = null;

        int count = 1 << 20;
        for (int i = 0; i < count; i++) {
            result = healthStatus.toString();
        }

        long end = System.currentTimeMillis();
        logger.info("{} us", (end - begin) * 1000 / count);

    }


    @Test
    public void testCalculateFromFistDelay(){

        HealthStatus healthStatus = new HealthStatus(hostPort, () -> downAfterMilli, () -> healthDelayMilli, scheduled);
        sleep((int) (downAfterMilli * 1.5));
        Assert.assertEquals(HEALTH_STATE.UNKNOWN, healthStatus.getState());

        healthStatus.delay(healthDelayMilli);
        Assert.assertEquals(HEALTH_STATE.UP, healthStatus.getState());
        sleep((int) (downAfterMilli * 1.5));
        Assert.assertEquals(HEALTH_STATE.DOWN, healthStatus.getState());

    }

}
