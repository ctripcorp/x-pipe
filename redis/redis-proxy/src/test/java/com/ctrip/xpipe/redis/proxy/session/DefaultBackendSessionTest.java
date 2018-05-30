package com.ctrip.xpipe.redis.proxy.session;

import com.ctrip.xpipe.redis.core.proxy.endpoint.DefaultProxyEndpoint;
import com.ctrip.xpipe.redis.core.proxy.endpoint.ProxyEndpoint;
import com.ctrip.xpipe.redis.core.proxy.endpoint.ProxyEndpointSelector;
import com.ctrip.xpipe.redis.core.proxy.handler.NettyClientSslHandlerFactory;
import com.ctrip.xpipe.redis.core.proxy.handler.NettyServerSslHandlerFactory;
import com.ctrip.xpipe.redis.core.proxy.handler.NettySslHandlerFactory;
import com.ctrip.xpipe.redis.proxy.TestProxyConfig;
import com.ctrip.xpipe.redis.proxy.Tunnel;
import com.ctrip.xpipe.redis.proxy.event.EventHandler;
import com.ctrip.xpipe.redis.proxy.exception.ResourceIncorrectException;
import com.ctrip.xpipe.redis.proxy.model.SessionMeta;
import com.ctrip.xpipe.redis.proxy.session.state.SessionEstablished;
import com.ctrip.xpipe.redis.proxy.session.state.SessionInit;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author chen.zhu
 * <p>
 * May 29, 2018
 */
public class DefaultBackendSessionTest {

    private DefaultBackendSession session;

    @Mock
    private Tunnel tunnel;

    @Mock
    private ProxyEndpointSelector selector;

    private NettySslHandlerFactory sslHandlerFactory = new NettyServerSslHandlerFactory(new TestProxyConfig());

    private EventLoopGroup eventLoopGroup = new NioEventLoopGroup(1, XpipeThreadFactory.create("Test-BackendSession"));

    @Before
    public void beforeDefaultBackendSessionTest() {
        MockitoAnnotations.initMocks(this);
        session = new DefaultBackendSession(tunnel, 300000, selector, eventLoopGroup, sslHandlerFactory);
    }

    @Test(expected = IllegalAccessException.class)
    public void testSendImmdiateAfterProtocol() throws Exception {
        session.sendImmdiateAfterProtocol(testByteBuf());
        session.sendImmdiateAfterProtocol(testByteBuf());
    }

    @Test
    public void getEndpoint() {
    }

    @Test
    public void onChannelEstablished() {
        EmbeddedChannel channel = new EmbeddedChannel();
        session.endpoint = new DefaultProxyEndpoint("TCP://127.0.0.1:6379");

        EventHandler handler = mock(EventHandler.class);
        session.registerChannelEstablishedHandler(handler);
        session.onChannelEstablished(channel);
        verify(handler).handle();
    }

    @Test(expected = ResourceIncorrectException.class)
    public void doStart() throws Exception {
        when(selector.selectCounts()).thenReturn(1);
        when(selector.getCandidates()).thenReturn(Lists.newArrayList());
        session.doStart();
    }

    @Test
    public void testDoStart() throws Exception {
        session.endpoint = new DefaultProxyEndpoint("TCP://127.0.0.1:6379");
        session.setSessionState(new SessionEstablished(session));
        session.doStart();
        verify(selector, never()).getCandidates();
    }

    @Test
    public void doSetSessionState() {
    }

    @Test
    public void getSessionType() {
        Assert.assertEquals(SESSION_TYPE.BACKEND, session.getSessionType());
    }

    @Test
    public void getSessionState() {
        Assert.assertEquals(new SessionInit(session), session.getSessionState());
    }

    private ByteBuf testByteBuf() {
        return Unpooled.copiedBuffer("TEST".getBytes());
    }
}