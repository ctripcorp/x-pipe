package com.ctrip.xpipe.redis.proxy.event;

import com.ctrip.xpipe.redis.proxy.Session;
import com.ctrip.xpipe.redis.proxy.session.SESSION_TYPE;
import com.ctrip.xpipe.redis.proxy.session.SessionStateChangeEvent;
import com.ctrip.xpipe.redis.proxy.session.state.SessionClosed;
import com.ctrip.xpipe.redis.proxy.session.state.SessionClosing;
import com.ctrip.xpipe.redis.proxy.tunnel.DefaultTunnel;
import com.ctrip.xpipe.redis.proxy.tunnel.state.BackendClosed;
import com.ctrip.xpipe.redis.proxy.tunnel.state.FrontendClosed;
import com.ctrip.xpipe.redis.proxy.tunnel.state.TunnelEstablished;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author chen.zhu
 * <p>
 * May 29, 2018
 */
public class SessionClosedEventHandlerTest {

    private SessionClosedEventHandler handler;

    @Mock
    private DefaultTunnel tunnel;

    @Mock
    private Session session;

    @Before
    public void beforeSessionClosedEventHandlerTest() {
        MockitoAnnotations.initMocks(this);
        when(session.tunnel()).thenReturn(tunnel);
    }

    @Test
    public void handle1() {
        SessionStateChangeEvent event = new SessionStateChangeEvent(new SessionClosing(session), new SessionClosed(session));
        when(session.getSessionType()).thenReturn(SESSION_TYPE.FRONTEND);
        when(tunnel.getState()).thenReturn(new TunnelEstablished(null));

        handler = new SessionClosedEventHandler(session, event);
        handler.handle();
        verify(tunnel).setState(new FrontendClosed(tunnel));
    }

    @Test
    public void handle2() {
        SessionStateChangeEvent event = new SessionStateChangeEvent(new SessionClosing(session), new SessionClosing(session));
        when(session.getSessionType()).thenReturn(SESSION_TYPE.FRONTEND);
        when(tunnel.getState()).thenReturn(new TunnelEstablished(null));

        handler = new SessionClosedEventHandler(session, event);
        handler.handle();
        verify(tunnel, never()).setState(any());
    }

    @Test
    public void handle3() {
        SessionStateChangeEvent event = new SessionStateChangeEvent(new SessionClosing(session), new SessionClosing(session));

        handler = new SessionClosedEventHandler(session, event);
        handler.handle();
        verify(tunnel, never()).setState(new FrontendClosed(tunnel));
    }

    @Test
    public void handle4() {
        SessionStateChangeEvent event = new SessionStateChangeEvent(new SessionClosing(session), new SessionClosed(session));
        when(session.getSessionType()).thenReturn(SESSION_TYPE.BACKEND);
        when(tunnel.getState()).thenReturn(new TunnelEstablished(null));

        handler = new SessionClosedEventHandler(session, event);
        handler.handle();
        verify(tunnel).setState(new BackendClosed(tunnel));
    }
}