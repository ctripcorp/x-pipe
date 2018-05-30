package com.ctrip.xpipe.redis.proxy.event;

import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.redis.proxy.tunnel.DefaultTunnel;
import com.ctrip.xpipe.redis.proxy.tunnel.TunnelStateChangeEvent;
import com.ctrip.xpipe.redis.proxy.tunnel.state.BackendClosed;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author chen.zhu
 * <p>
 * May 29, 2018
 */
public class AbstractTunnelEventHandlerTest {

    @Mock
    private DefaultTunnel tunnel;

    @Mock
    private TunnelStateChangeEvent event;

    private AbstractTunnelEventHandler handler;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        when(event.getCurrent()).thenReturn(new BackendClosed(tunnel));
    }

    @Test
    public void handle() {
        EventHandler mockHandler = mock(EventHandler.class);
        handler = new AbstractTunnelEventHandler(tunnel, event) {
            @Override
            protected void doHandle() {
                mockHandler.handle();
            }
        };
        handler.handle();
        verify(mockHandler).handle();
    }

    @Test(expected = XpipeRuntimeException.class)
    public void doHandle() {
        handler = new AbstractTunnelEventHandler(tunnel, event) {
            @Override
            protected void doHandle() {
                throw new XpipeRuntimeException("expected exception");
            }
        };
        handler.handle();
    }
}