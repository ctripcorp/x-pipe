package com.ctrip.xpipe.redis.proxy.handler;

import com.ctrip.xpipe.redis.core.proxy.parser.DefaultProxyConnectProtocolParser;
import com.ctrip.xpipe.redis.proxy.AbstractNettyTest;
import com.ctrip.xpipe.redis.proxy.exception.ResourceIncorrectException;
import com.ctrip.xpipe.redis.proxy.exception.WriteToClosedSessionException;
import com.ctrip.xpipe.redis.proxy.session.DefaultFrontendSession;
import com.ctrip.xpipe.redis.proxy.tunnel.DefaultTunnel;
import com.ctrip.xpipe.redis.proxy.tunnel.DefaultTunnelManager;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.ReferenceCountUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
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
public class FrontendSessionNettyHandlerTest extends AbstractNettyTest {

    @Mock
    private DefaultTunnelManager manager;

    @Mock
    private DefaultTunnel tunnel;

    @Mock
    private DefaultFrontendSession session;

    private FrontendSessionNettyHandler handler;

    private EmbeddedChannel channel;

    @Before
    public void beforeFrontendSessionNettyHandlerTest() {
        MockitoAnnotations.initMocks(this);
        when(manager.create(any(), any())).thenReturn(tunnel);
        when(tunnel.frontend()).thenReturn(session);
        handler = new FrontendSessionNettyHandler(tunnel);
        channel = new EmbeddedChannel(handler);
    }

    @Test
    public void testChannelInactive() {
        channel.finish();

    }

    @Test
    public void testChannelRead() {
        Throwable throwable = null;
        logger.info("[channelRead] exception expected");
        try {
            channel.writeInbound(new DefaultProxyConnectProtocolParser().read("PROXY ROUTE TCP://127.0.0.1:6379"));
            channel.checkException();
        } catch (Exception e) {
            throwable = e;
        }
        Assert.assertNotNull(throwable);
        Assert.assertTrue(throwable instanceof ResourceIncorrectException);

        verify(session).release();
    }

    @Test
    public void testChannelRead2() {
        String expected = "Hello World";
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                Object object = invocation.getArguments()[0];
                Assert.assertTrue(object instanceof ByteBuf);
                Assert.assertEquals(expected, ((ByteBuf)object).toString(Charset.defaultCharset()));
                return null;
            }
        }).when(tunnel).forwardToBackend(any());
        channel.writeInbound(Unpooled.copiedBuffer(expected.getBytes()));
        verify(manager, never()).create(any(), any());
        verify(tunnel).forwardToBackend(any());
    }

    @Test
    public void testChannelRead3() {
        Throwable throwable = null;
        logger.info("[channelRead][exception expected]");
        try {
            channel.writeInbound("Hello Wrold");
        } catch (Exception e) {
            throwable = e;
        }
        Assert.assertNotNull(throwable);
        Assert.assertTrue(throwable instanceof ResourceIncorrectException);
        verify(tunnel, never()).forwardToBackend(any());
    }

    @Test
    public void testByteBufReleasedAfterPipelineBroken() {
        Throwable th = new WriteToClosedSessionException("session closed");
        doThrow(th).when(tunnel).forwardToBackend(any());

        ByteBuf byteBuf = Unpooled.copiedBuffer("test".getBytes());
        channel.writeInbound(byteBuf);
        Assert.assertEquals(0, byteBuf.refCnt());
        Mockito.verify(session).release();
    }

}