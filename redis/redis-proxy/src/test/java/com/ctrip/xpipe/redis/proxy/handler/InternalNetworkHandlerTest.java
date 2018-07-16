package com.ctrip.xpipe.redis.proxy.handler;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.junit.Test;

import java.net.InetSocketAddress;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author chen.zhu
 * <p>
 * Jul 16, 2018
 */
public class InternalNetworkHandlerTest {

    @Test
    public void testChannelActiveWithInternalNetwork() throws Exception {
        InternalNetworkHandler handler = new InternalNetworkHandler();
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        Channel channel = mock(Channel.class);
        when(channel.remoteAddress()).thenReturn(new InetSocketAddress("10.2.1.1", 1234));
        when(ctx.channel()).thenReturn(channel);
        handler.channelActive(ctx);
        verify(channel, never()).close();
    }

    @Test
    public void testChannelActiveWithOutterNetwork() throws Exception {
        InternalNetworkHandler handler = new InternalNetworkHandler();
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        Channel channel = mock(Channel.class);
        when(channel.remoteAddress()).thenReturn(new InetSocketAddress("101.2.1.1", 1234));
        when(ctx.channel()).thenReturn(channel);
        handler.channelActive(ctx);
        verify(channel).close();
    }
}