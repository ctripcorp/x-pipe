
package com.ctrip.xpipe.redis.proxy.session;

import com.ctrip.xpipe.redis.proxy.Session;
import com.ctrip.xpipe.redis.proxy.Tunnel;
import com.ctrip.xpipe.redis.proxy.config.ProxyConfig;
import com.ctrip.xpipe.redis.proxy.integrate.AbstractProxyIntegrationTest;
import com.ctrip.xpipe.redis.proxy.model.TunnelIdentity;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author chen.zhu
 * <p>
 * Sep 25, 2018
 */
public class SessionWritableEventHandlerTest extends AbstractProxyIntegrationTest {

    private SessionWritableEventHandler handler;

    private Session session;

    private ProxyConfig config;

    @Before
    public void beforeSessionWritableEventHandlerTest() {
        session = mock(Session.class);
        config = mock(ProxyConfig.class);
        when(config.getCloseChannelAfterReadCloseMilli()).thenReturn(10);
        handler = new SessionWritableEventHandler(session, scheduled, config);
    }

    @Test
    public void testOnInit() {

    }

    @Test
    public void testOnEstablished() {
    }

    @Test
    public void testOnWritable() {
        when(config.getCloseChannelAfterReadCloseMilli()).thenReturn(10);
        handler.onNotWritable();
        sleep(5);
        handler.onWritable();
        sleep(10);
        verify(session, never()).release();
    }

    @Test
    public void testOnNotWritable() {
        when(config.getCloseChannelAfterReadCloseMilli()).thenReturn(0);
        Tunnel tunnel = mock(Tunnel.class);
        when(session.tunnel()).thenReturn(tunnel);
        when(tunnel.identity()).thenReturn(mock(TunnelIdentity.class));
        handler.onNotWritable();
        sleep(20);
        verify(session, times(1)).release();
    }
}