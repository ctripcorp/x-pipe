package com.ctrip.xpipe.redis.core.proxy.endpoint;

import com.ctrip.xpipe.netty.TcpPortCheckCommand;
import com.ctrip.xpipe.proxy.ProxyEndpoint;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.simpleserver.Server;
import org.junit.*;

import java.util.UUID;


/**
 * @author chen.zhu
 * <p>
 * Oct 15, 2018
 */
public class DefaultProxyEndpointHealthCheckerTest extends AbstractRedisTest {

    private DefaultProxyEndpointHealthChecker checker;

    private Server server;

    private ProxyEndpoint endpoint;

    private ProxyEndpoint normalEndpoint;

    private int mockDropEndpointTime = 1000;

    private long originDropEndpointTime;

    private int mockCheckInterval = 100;

    private int originCheckInterval;

    private int mockConnectTimeout = 1000;

    private int originConnectTimeout;

    @Before
    public void beforeDefaultEndpointHealthCheckerTest() throws Exception {
        checker = new DefaultProxyEndpointHealthChecker(scheduled);
        server = startEmptyServer();
        endpoint = new DefaultProxyEndpoint("PROXYTCP://127.0.0.1:" + server.getPort());
        normalEndpoint = new DefaultProxyEndpoint("127.0.0.1", server.getPort());
        this.originDropEndpointTime = DefaultProxyEndpointHealthChecker.DEFAULT_DROP_ENDPOINT_INTERVAL_MILLI;
        this.originCheckInterval = DefaultProxyEndpointHealthChecker.ENDPOINT_HEALTH_CHECK_INTERVAL;
        this.originConnectTimeout = TcpPortCheckCommand.CHECK_TIMEOUT_MILLI;
        DefaultProxyEndpointHealthChecker.DEFAULT_DROP_ENDPOINT_INTERVAL_MILLI = this.mockDropEndpointTime;
        DefaultProxyEndpointHealthChecker.ENDPOINT_HEALTH_CHECK_INTERVAL = this.mockCheckInterval;
        TcpPortCheckCommand.CHECK_TIMEOUT_MILLI = mockConnectTimeout;
    }

    @After
    public void afterDefaultEndpointHealthCheckerTest() throws Exception {
        DefaultProxyEndpointHealthChecker.DEFAULT_DROP_ENDPOINT_INTERVAL_MILLI = this.originDropEndpointTime;
        DefaultProxyEndpointHealthChecker.ENDPOINT_HEALTH_CHECK_INTERVAL = this.originCheckInterval;
        TcpPortCheckCommand.CHECK_TIMEOUT_MILLI = this.originConnectTimeout;
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

    @Test
    public void testNormalEndpointCheckConnectivity() {
        boolean status = checker.checkConnectivity(normalEndpoint);
        Assert.assertTrue(status);
        Assert.assertFalse(checker.getAllHealthStatus().containsKey(normalEndpoint));
    }

    @Ignore
    @Test
    public void testCheckConnectivityWithSeveralTimeLater() {
        boolean status = checker.checkConnectivity(endpoint);
        Assert.assertTrue(status);
        Assert.assertTrue(checker.getAllHealthStatus().containsKey(endpoint));
        sleep(mockDropEndpointTime);
        Assert.assertTrue(checker.checkConnectivity(endpoint));
        logger.info("{}", checker.getAllHealthStatus().get(endpoint));
    }

    @Ignore
    @Test
    public void testOverTime() throws Exception {
        checker.checkConnectivity(endpoint);
        sleep(mockCheckInterval);
        Assert.assertTrue(checker.checkConnectivity(endpoint));
        Assert.assertTrue(checker.getAllHealthStatus().containsKey(endpoint));

        server.stop();
        sleep(mockDropEndpointTime);
        Assert.assertFalse(checker.getAllHealthStatus().containsKey(endpoint));

    }


    @Ignore
    @Test
    public void testHealthStateChange() throws Exception {
        checker.checkConnectivity(endpoint);
        DefaultProxyEndpointHealthChecker.EndpointHealthState state = checker.getAllHealthStatus().get(endpoint).getHealthState();
        Assert.assertEquals(DefaultProxyEndpointHealthChecker.EndpointHealthState.UNKNOWN, state);

        sleep(2 * mockDropEndpointTime);
        state = checker.getAllHealthStatus().get(endpoint).getHealthState();
        Assert.assertEquals(DefaultProxyEndpointHealthChecker.EndpointHealthState.HEALTHY, state);
        Assert.assertTrue(checker.checkConnectivity(endpoint));
        logger.info("[jump out of]");

        server.stop();
        sleep(mockCheckInterval);
        state = checker.getAllHealthStatus().get(endpoint).getHealthState();
        Assert.assertEquals(DefaultProxyEndpointHealthChecker.EndpointHealthState.FAIL_ONCE, state);
        Assert.assertTrue(checker.checkConnectivity(endpoint));


        sleep(mockCheckInterval);
        state = checker.getAllHealthStatus().get(endpoint).getHealthState();
        Assert.assertEquals(DefaultProxyEndpointHealthChecker.EndpointHealthState.FAIL_TWICE, state);
        Assert.assertTrue(checker.checkConnectivity(endpoint));

        sleep(mockCheckInterval * 2);
        state = checker.getAllHealthStatus().get(endpoint).getHealthState();
        Assert.assertEquals(DefaultProxyEndpointHealthChecker.EndpointHealthState.UNHEALTHY, state);
        Assert.assertFalse(checker.checkConnectivity(endpoint));
    }

    @Ignore
    @Test
    public void testCancelPingAfterRemove() throws Exception {
        checker.checkConnectivity(endpoint);

        sleep(mockCheckInterval);
        Assert.assertTrue(checker.checkConnectivity(endpoint));
        sleep(mockCheckInterval * 2);

        server.stop();
        sleep(mockDropEndpointTime);
        Assert.assertFalse(checker.getAllHealthStatus().containsKey(endpoint));

        sleep(mockDropEndpointTime);
    }

    @Test
    public void testResetCheckerAfterLongDown() throws Exception {
        server.stop();
        checker.checkConnectivity(endpoint);
        sleep(mockDropEndpointTime + mockCheckInterval + 10);

        Assert.assertFalse(checker.checkConnectivity(endpoint));
        checker.resetIfNeed(endpoint);
        Assert.assertTrue(checker.getAllHealthStatus().isEmpty());
    }

    @Test
    public void testSingleCheckCommand() throws Exception {
        ProxyEndpoint endpoint = new DefaultProxyEndpoint("PROXYTCP://10.0.0.1:1000"); // for connect timeout
        DefaultProxyEndpointHealthChecker.EndpointHealthStatus status = checker.new EndpointHealthStatus(endpoint);

        // HEALTHY -> FAIL ONCE -> FAIL TWICE -> UNHEALTHY
        status.check();
        status.check();
        status.check();

        try {
            status.getCurrentCheckFuture().get();
        } catch (Throwable th) {
            // ignore
        }

        Assert.assertTrue(status.isHealthy());
    }

}