package com.ctrip.xpipe.redis.proxy.event;

import com.ctrip.xpipe.redis.proxy.session.DefaultBackendSession;
import com.ctrip.xpipe.redis.proxy.session.DefaultFrontendSession;
import com.ctrip.xpipe.redis.proxy.tunnel.DefaultTunnel;
import com.ctrip.xpipe.redis.proxy.tunnel.TunnelStateChangeEvent;
import com.ctrip.xpipe.redis.proxy.tunnel.state.BackendClosed;
import com.ctrip.xpipe.redis.proxy.tunnel.state.FrontendClosed;
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
public class TunnelFrontendClosedEventHandlerTest {

    private TunnelFrontendClosedEventHandler handler;

    @Mock
    private DefaultTunnel tunnel;

    @Mock
    private DefaultBackendSession session;

    @Before
    public void beforeTunnelBackendClosedEventHandlerTest() {
        MockitoAnnotations.initMocks(this);
        when(tunnel.backend()).thenReturn(session);
    }

    @Test
    public void doHandle() {
        TunnelStateChangeEvent event = new TunnelStateChangeEvent(new TunnelEstablished(tunnel), new FrontendClosed(tunnel));
        handler = new TunnelFrontendClosedEventHandler(tunnel, event);
        doNothing().when(session).release();

        handler.handle();

        verify(session).release();
        verify(tunnel).setState(new TunnelClosing(tunnel));
    }
}