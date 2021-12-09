package com.ctrip.xpipe.redis.proxy.monitor.tunnel;

import com.ctrip.xpipe.redis.proxy.AbstractRedisProxyServerTest;
import com.ctrip.xpipe.redis.proxy.TestProxyConfig;
import com.ctrip.xpipe.redis.proxy.Tunnel;
import com.ctrip.xpipe.redis.proxy.monitor.DefaultTunnelMonitorManager;
import com.ctrip.xpipe.redis.proxy.tunnel.DefaultTunnel;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.mockito.Mockito.*;

/**
 * @author chen.zhu
 * <p>
 * Oct 29, 2018
 */
public class DefaultTunnelMonitorTest extends AbstractRedisProxyServerTest {

    private Tunnel tunnel;

    private DefaultTunnelMonitor monitor;

    @Before
    public void beforeDefaultTunnelMonitorTest() throws Exception {
        tunnel = mock(Tunnel.class);
        TestProxyConfig testProxyConfig = (TestProxyConfig) proxyResourceManager.getProxyConfig();
        monitor = new DefaultTunnelMonitor(proxyResourceManager, tunnel, new DefaultTunnelRecorder());
    }

    @Test
    public void testGetFrontendSessionMonitor() {
        Assert.assertNotNull(monitor.getFrontendSessionMonitor());
    }

    @Test
    public void testGetBackendSessionMonitor() {
        Assert.assertNotNull(monitor.getBackendSessionMonitor());
    }

    @Test
    public void testGetTunnelStats() {
        Assert.assertNotNull(monitor.getTunnelStats());
    }

    //Manual Test: to check whether a proper log is printed
    @Ignore
    @Test
    public void recordThrowException() throws Exception {
        tunnel = new DefaultTunnel(new EmbeddedChannel(), protocol(), new TestProxyConfig(),
                proxyResourceManager, new DefaultTunnelMonitorManager(proxyResourceManager));
        monitor = new DefaultTunnelMonitor(proxyResourceManager, tunnel, new DefaultTunnelRecorder());
        DefaultTunnelMonitor spy = spy(monitor);
        doThrow(new RuntimeException()).when(spy).record(any());
        spy.scheduleRecord();
        Thread.currentThread().join();
    }
}