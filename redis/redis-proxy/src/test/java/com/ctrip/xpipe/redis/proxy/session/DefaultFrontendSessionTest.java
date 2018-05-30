package com.ctrip.xpipe.redis.proxy.session;

import com.ctrip.xpipe.redis.proxy.Tunnel;
import com.ctrip.xpipe.redis.proxy.event.EventHandler;
import com.ctrip.xpipe.redis.proxy.session.state.SessionEstablished;
import com.ctrip.xpipe.redis.proxy.tunnel.DefaultTunnel;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.nio.NioEventLoop;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.ImmediateEventExecutor;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.Queue;

import static io.netty.util.concurrent.ImmediateEventExecutor.INSTANCE;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * @author chen.zhu
 * <p>
 * May 29, 2018
 */
public class DefaultFrontendSessionTest {

    private DefaultFrontendSession session;

    private EmbeddedChannel channel = new EmbeddedChannel();

    @Mock
    private DefaultTunnel tunnel;

    @Mock
    private DefaultBackendSession backend;

    private Queue<EventHandler> queue = new LinkedList<>();

    @Before
    public void beforeDefaultFrontendSessionTest() {
        MockitoAnnotations.initMocks(this);
        channel = spy(channel);
        when(channel.eventLoop()).thenReturn(new NioEventLoopGroup(1).next());
        when(channel.remoteAddress()).thenReturn(new InetSocketAddress("127.0.0.1", 6379));
        session = new DefaultFrontendSession(tunnel, channel, 300000);
        when(tunnel.backend()).thenReturn(backend);
        when(tunnel.frontend()).thenReturn(session);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                queue.offer((EventHandler) invocation.getArguments()[0]);
                return null;
            }
        }).when(backend).registerChannelEstablishedHandler(any(EventHandler.class));
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                while(!queue.isEmpty()) {
                    queue.poll().handle();
                }
                return null;
            }
        }).when(backend).onChannelEstablished(any());
    }

    @Test
    public void testGetSessionType() {
        Assert.assertEquals(SESSION_TYPE.FRONTEND, session.getSessionType());
    }

    @Test
    public void testDoInitialize() throws Exception {
        doCallRealMethod().when(tunnel).closeFrontendRead();
        doCallRealMethod().when(tunnel).triggerFrontendRead();
        Assert.assertTrue(channel.config().isAutoRead());
        session.doInitialize();
        Assert.assertFalse(channel.config().isAutoRead());
        Assert.assertEquals(1, queue.size());
        backend.onChannelEstablished(new EmbeddedChannel());
        Thread.sleep(2);
        Assert.assertTrue(channel.config().isAutoRead());
    }

    @Test
    public void testGetSessionState() {
        Assert.assertEquals(new SessionEstablished(session), session.getSessionState());
    }
}