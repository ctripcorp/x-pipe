package com.ctrip.xpipe.redis.core.proxy.endpoint;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.simpleserver.Server;
import org.junit.*;

/**
 * @author chen.zhu
 * <p>
 * Oct 15, 2018
 */
public class DefaultEndpointHealthCheckerTest extends AbstractRedisTest {

    private DefaultEndpointHealthChecker checker;

    private Server server;

    private Endpoint endpoint;

    @Before
    public void beforeDefaultEndpointHealthCheckerTest() throws Exception {
        checker = new DefaultEndpointHealthChecker(scheduled);
        server = startEmptyServer();
        endpoint = localhostEndpoint(server.getPort());
        DefaultEndpointHealthChecker.DEFAULT_DROP_ENDPOINT_INTERVAL_MILLI = 1000 * 10;
    }

    @After
    public void afterDefaultEndpointHealthCheckerTest() throws Exception {
        if(server != null && server.getLifecycleState().canStop()) {
            server.stop();
        }
    }

    @Test
    public void testCheckConnectivity() {
        boolean status = checker.checkConnectivity(endpoint);
        Assert.assertTrue(status);
        Assert.assertTrue(checker.getAllHealthStatus().containsKey(endpoint));
    }

    @Ignore
    @Test
    public void testCheckConnectivityWithSeveralTimeLater() {
        boolean status = checker.checkConnectivity(endpoint);
        Assert.assertTrue(status);
        Assert.assertTrue(checker.getAllHealthStatus().containsKey(endpoint));
        sleep(10 * 1000);
        Assert.assertTrue(checker.checkConnectivity(endpoint));
        logger.info("{}", checker.getAllHealthStatus().get(endpoint));
    }

    @Ignore
    @Test
    public void testOverTime() throws Exception {
        checker.checkConnectivity(endpoint);
        sleep(1000);
        Assert.assertTrue(checker.checkConnectivity(endpoint));
        Assert.assertTrue(checker.getAllHealthStatus().containsKey(endpoint));

        server.stop();
        sleep(1000 * 10);
        Assert.assertFalse(checker.getAllHealthStatus().containsKey(endpoint));

    }


    @Ignore
    @Test
    public void testHealthStateChange() throws Exception {
        checker.checkConnectivity(endpoint);
        DefaultEndpointHealthChecker.EndpointHealthState state = checker.getAllHealthStatus().get(endpoint).getHealthState();
        Assert.assertEquals(DefaultEndpointHealthChecker.EndpointHealthState.UNKNOWN, state);

        sleep(10 * 2000);
        state = checker.getAllHealthStatus().get(endpoint).getHealthState();
        Assert.assertEquals(DefaultEndpointHealthChecker.EndpointHealthState.HEALTHY, state);
        Assert.assertTrue(checker.checkConnectivity(endpoint));
        logger.info("[jump out of]");

        server.stop();
        sleep(1000);
        state = checker.getAllHealthStatus().get(endpoint).getHealthState();
        Assert.assertEquals(DefaultEndpointHealthChecker.EndpointHealthState.FAIL_ONCE, state);
        Assert.assertTrue(checker.checkConnectivity(endpoint));


        sleep(1000);
        state = checker.getAllHealthStatus().get(endpoint).getHealthState();
        Assert.assertEquals(DefaultEndpointHealthChecker.EndpointHealthState.FAIL_TWICE, state);
        Assert.assertTrue(checker.checkConnectivity(endpoint));

        sleep(1100);
        state = checker.getAllHealthStatus().get(endpoint).getHealthState();
        Assert.assertEquals(DefaultEndpointHealthChecker.EndpointHealthState.UNHEALTHY, state);
        Assert.assertFalse(checker.checkConnectivity(endpoint));
    }

    @Ignore
    @Test
    public void testCancelPingAfterRemove() throws Exception {
        checker.checkConnectivity(endpoint);

        sleep(1000);
        Assert.assertTrue(checker.checkConnectivity(endpoint));
        sleep(2000);

        server.stop();
        sleep(10 * 1000);
        Assert.assertFalse(checker.getAllHealthStatus().containsKey(endpoint));

        sleep(10 * 1000);
    }
}