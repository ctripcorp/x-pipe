package com.ctrip.xpipe.redis.proxy.monitor.session;

import com.ctrip.xpipe.redis.proxy.Session;
import com.ctrip.xpipe.redis.proxy.integrate.AbstractProxyIntegrationTest;
import com.ctrip.xpipe.redis.proxy.resource.TestResourceManager;
import com.ctrip.xpipe.redis.proxy.session.BackendSession;
import org.junit.Assert;
import org.junit.Test;

import static org.mockito.Mockito.mock;

public class DefaultSessionMonitorTest extends AbstractProxyIntegrationTest {

    private Session session = mock(BackendSession.class);

    private DefaultSessionMonitor sessionMonitor = new DefaultSessionMonitor(new TestResourceManager(), session);

    @Test
    public void testGetSessionStats() {
        Assert.assertNotNull(sessionMonitor.getSessionStats());
    }

    @Test
    public void testGetOutboundBufferMonitor() {
        Assert.assertNotNull(sessionMonitor.getOutboundBufferMonitor());
    }

    @Test
    public void testGetSocketStats() {
        Assert.assertNotNull(sessionMonitor.getSocketStats());
    }

}