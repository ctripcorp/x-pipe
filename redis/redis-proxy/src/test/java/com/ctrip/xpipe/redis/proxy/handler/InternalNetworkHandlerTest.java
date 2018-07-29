package com.ctrip.xpipe.redis.proxy.handler;

import com.ctrip.xpipe.redis.proxy.config.ProxyConfig;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.net.InetSocketAddress;

import static org.mockito.Mockito.*;

/**
 * @author chen.zhu
 * <p>
 * Jul 16, 2018
 */
public class InternalNetworkHandlerTest {

    @Mock
    private ProxyConfig config;

    @Before
    public void beforeInternalNetworkHandlerTest() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testChannelActiveWithInternalNetwork() throws Exception {
        when(config.getInternalNetworkPrefix()).thenReturn(new String[]{"10"});
        InternalNetworkHandler handler = new InternalNetworkHandler(config.getInternalNetworkPrefix());
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        Channel channel = mock(Channel.class);
        when(channel.remoteAddress()).thenReturn(new InetSocketAddress("10.2.1.1", 1234));
        when(ctx.channel()).thenReturn(channel);
        handler.channelActive(ctx);
        verify(channel, never()).close();
    }

    @Test
    public void testChannelActiveWithOutterNetwork() throws Exception {
        when(config.getInternalNetworkPrefix()).thenReturn(new String[]{"10", "26"});
        InternalNetworkHandler handler = new InternalNetworkHandler(config.getInternalNetworkPrefix());
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        Channel channel = mock(Channel.class);
        when(channel.remoteAddress()).thenReturn(new InetSocketAddress("101.2.1.1", 1234));
        when(ctx.channel()).thenReturn(channel);
        handler.channelActive(ctx);
        verify(channel).close();
    }

    @Test
    public void testChannelActiveWithNoPrefixSupported() throws Exception {
        when(config.getInternalNetworkPrefix()).thenReturn(null);
        InternalNetworkHandler handler = new InternalNetworkHandler(config.getInternalNetworkPrefix());
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        Channel channel = mock(Channel.class);
        when(channel.remoteAddress()).thenReturn(new InetSocketAddress("101.2.1.1", 1234));
        when(ctx.channel()).thenReturn(channel);
        handler.channelActive(ctx);
        verify(channel, never()).close();
    }
}