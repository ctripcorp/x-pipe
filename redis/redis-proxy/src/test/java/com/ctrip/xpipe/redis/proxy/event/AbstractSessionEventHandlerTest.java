package com.ctrip.xpipe.redis.proxy.event;

import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.redis.proxy.Session;
import com.ctrip.xpipe.redis.proxy.session.SessionStateChangeEvent;
import com.ctrip.xpipe.redis.proxy.session.state.SessionEstablished;
import com.ctrip.xpipe.redis.proxy.session.state.SessionInit;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author chen.zhu
 * <p>
 * May 29, 2018
 */
public class AbstractSessionEventHandlerTest {

    private static Logger logger = LoggerFactory.getLogger(AbstractSessionEventHandlerTest.class);

    @Mock
    private Session session;

    private SessionStateChangeEvent event = new SessionStateChangeEvent(new SessionInit(session),
            new SessionEstablished(session));

    AbstractSessionEventHandler handler;

    @Before
    public void beforeAbstractSessionEventHandlerTest() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void handle() {
        EventHandler mockHandler = mock(EventHandler.class);

        handler  = new AbstractSessionEventHandler(session, event) {
            @Override
            protected void doHandle() {
                mockHandler.handle();
            }
        };
        handler.handle();
        verify(mockHandler, atLeastOnce()).handle();
    }

    @Test(expected = XpipeRuntimeException.class)
    public void doHandle() {
        handler  = new AbstractSessionEventHandler(session, event) {
            @Override
            protected void doHandle() {
                throw new XpipeRuntimeException("expected exception");
            }
        };
        handler.handle();
    }
}