package com.ctrip.xpipe.redis.proxy.event;

import com.ctrip.xpipe.redis.proxy.session.DefaultFrontendSession;
import com.ctrip.xpipe.redis.proxy.tunnel.DefaultTunnel;
import com.ctrip.xpipe.redis.proxy.tunnel.TunnelStateChangeEvent;
import com.ctrip.xpipe.redis.proxy.tunnel.state.BackendClosed;
import com.ctrip.xpipe.redis.proxy.tunnel.state.TunnelClosing;
import com.ctrip.xpipe.redis.proxy.tunnel.state.TunnelEstablished;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.*;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author chen.zhu
 * <p>
 * May 29, 2018
 */
public class TunnelBackendClosedEventHandlerTest {

    private TunnelBackendClosedEventHandler handler;

    @Mock
    private DefaultTunnel tunnel;

    @Mock
    private DefaultFrontendSession session;

    @Before
    public void beforeTunnelBackendClosedEventHandlerTest() {
        MockitoAnnotations.initMocks(this);
        when(tunnel.frontend()).thenReturn(session);
    }

    @Test
    public void doHandle() {
        TunnelStateChangeEvent event = new TunnelStateChangeEvent(new TunnelEstablished(tunnel), new BackendClosed(tunnel));
        handler = new TunnelBackendClosedEventHandler(tunnel, event);
        doNothing().when(session).release();

        handler.handle();

        verify(session).release();
        verify(tunnel).setState(new TunnelClosing(tunnel));
    }
}