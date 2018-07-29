package com.ctrip.xpipe.redis.proxy.handler;

import com.ctrip.xpipe.redis.proxy.AbstractNettyTest;
import com.ctrip.xpipe.redis.proxy.session.DefaultBackendSession;
import com.ctrip.xpipe.redis.proxy.session.DefaultFrontendSession;
import com.ctrip.xpipe.redis.proxy.tunnel.DefaultTunnel;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.nio.charset.Charset;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * @author chen.zhu
 * <p>
 * May 29, 2018
 */
public class BackendSessionHandlerTest extends AbstractNettyTest {

    @Mock
    private DefaultTunnel tunnel;

    @Mock
    private DefaultBackendSession session;

    @Mock
    private DefaultFrontendSession frontendSession;

    private BackendSessionHandler handler;

    private EmbeddedChannel channel;

    @Before
    public void beforeFrontendSessionNettyHandlerTest() {
        MockitoAnnotations.initMocks(this);
        when(tunnel.backend()).thenReturn(session);
        when(tunnel.frontend()).thenReturn(frontendSession);
        handler = new BackendSessionHandler(tunnel);
        channel = new EmbeddedChannel(handler);
    }

    @Test
    public void channelRead() {
        String expected = "HEllo world";
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                Object object = invocation.getArguments()[0];
                Assert.assertTrue(object instanceof ByteBuf);
                Assert.assertEquals(expected, ((ByteBuf)object).toString(Charset.defaultCharset()));
                return null;
            }
        }).when(tunnel).forwardToFrontend(any());

        channel.writeInbound(Unpooled.copiedBuffer(expected.getBytes()));

        verify(tunnel).forwardToFrontend(any());
    }

}