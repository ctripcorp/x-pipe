package com.ctrip.xpipe.redis.proxy.event;

import com.ctrip.xpipe.redis.proxy.Session;
import com.ctrip.xpipe.redis.proxy.session.SESSION_TYPE;
import com.ctrip.xpipe.redis.proxy.session.SessionStateChangeEvent;
import com.ctrip.xpipe.redis.proxy.session.state.SessionEstablished;
import com.ctrip.xpipe.redis.proxy.session.state.SessionInit;
import com.ctrip.xpipe.redis.proxy.tunnel.DefaultTunnel;
import com.ctrip.xpipe.redis.proxy.tunnel.state.TunnelEstablished;
import com.ctrip.xpipe.redis.proxy.tunnel.state.TunnelHalfEstablished;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.*;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author chen.zhu
 * <p>
 * May 29, 2018
 */
public class SessionEstablishedHandlerTest {

    private SessionEstablishedHandler handler;

    @Mock
    private DefaultTunnel tunnel;

    @Mock
    private Session session;

    @Before
    public void beforeSessionEstablishedHandlerTest() {
        MockitoAnnotations.initMocks(this);
        when(session.tunnel()).thenReturn(tunnel);
    }

    @Test
    public void doHandle() {
        SessionStateChangeEvent event = new SessionStateChangeEvent(new SessionInit(session), new SessionEstablished(session));
        when(session.getSessionType()).thenReturn(SESSION_TYPE.BACKEND);
        when(tunnel.getState()).thenReturn(new TunnelHalfEstablished(tunnel));
        handler = new SessionEstablishedHandler(session, event);
        handler.handle();

        verify(tunnel).setState(new TunnelEstablished(tunnel));
    }

    @Test
    public void doHandle2() {
        SessionStateChangeEvent event = new SessionStateChangeEvent(new SessionInit(session), new SessionEstablished(session));
        when(session.getSessionType()).thenReturn(SESSION_TYPE.FRONTEND);
        when(tunnel.getState()).thenReturn(new TunnelHalfEstablished(tunnel));
        handler = new SessionEstablishedHandler(session, event);
        handler.handle();

        verify(tunnel, never()).setState(new TunnelEstablished(tunnel));
    }

    @Test
    public void doHandle3() {
        SessionStateChangeEvent event = new SessionStateChangeEvent(new SessionInit(session), new SessionEstablished(session));
        when(session.getSessionType()).thenReturn(SESSION_TYPE.BACKEND);
        when(tunnel.getState()).thenReturn(new TunnelEstablished(tunnel));
        handler = new SessionEstablishedHandler(session, event);
        handler.handle();

        verify(tunnel, never()).setState(new TunnelEstablished(tunnel));
    }
}