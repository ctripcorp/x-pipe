package com.ctrip.xpipe.redis.proxy.event;

import com.ctrip.xpipe.redis.proxy.session.DefaultBackendSession;
import com.ctrip.xpipe.redis.proxy.session.SessionStateChangeEvent;
import com.ctrip.xpipe.redis.proxy.session.state.SessionClosed;
import com.ctrip.xpipe.redis.proxy.session.state.SessionClosing;
import com.ctrip.xpipe.redis.proxy.session.state.SessionEstablished;
import com.ctrip.xpipe.redis.proxy.tunnel.DefaultTunnel;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.*;

/**
 * @author chen.zhu
 * <p>
 * May 29, 2018
 */
public class SessionClosingEventHandlerTest {

    private SessionClosingEventHandler handler;

    @Mock
    private DefaultTunnel tunnel;

    @Mock
    private DefaultBackendSession session;

    @Before
    public void beforeSessionClosingEventHandlerTest() {
        MockitoAnnotations.initMocks(this);
        when(session.tunnel()).thenReturn(tunnel);
    }

    @Test
    public void doHandle() {
        SessionStateChangeEvent event = new SessionStateChangeEvent(new SessionEstablished(session),
                new SessionClosing(session));
        doNothing().when(session).disconnect();
        handler = new SessionClosingEventHandler(session, event);

        handler.handle();

        verify(session, atLeastOnce()).disconnect();
        verify(session).setSessionState(new SessionClosed(session));
    }
}