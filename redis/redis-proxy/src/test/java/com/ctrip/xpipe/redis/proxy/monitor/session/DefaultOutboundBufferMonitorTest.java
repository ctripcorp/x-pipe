package com.ctrip.xpipe.redis.proxy.monitor.session;

import com.ctrip.xpipe.redis.proxy.Session;
import com.ctrip.xpipe.redis.proxy.integrate.AbstractProxyIntegrationTest;
import io.netty.channel.Channel;
import org.junit.Assert;
import org.junit.Test;

import static org.mockito.Mockito.when;

public class DefaultOutboundBufferMonitorTest extends AbstractProxyIntegrationTest {

    private Session session = mockSession();

    private DefaultOutboundBufferMonitor monitor = new DefaultOutboundBufferMonitor(session);

    @Test
    public void testGetOutboundBufferCumulation() {
        Channel channel = mockChannel();
        when(channel.isActive()).thenReturn(false);
        when(session.getChannel()).thenReturn(channel);
        long outboundBufferSize = monitor.getOutboundBufferCumulation();
        Assert.assertEquals(-1L, outboundBufferSize);
    }
}