package com.ctrip.xpipe.redis.proxy.handler;

import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.redis.proxy.AbstractNettyTest;
import com.ctrip.xpipe.redis.proxy.Session;
import com.ctrip.xpipe.redis.proxy.Tunnel;
import com.ctrip.xpipe.redis.proxy.session.state.SessionClosed;
import com.ctrip.xpipe.redis.proxy.session.state.SessionClosing;
import com.ctrip.xpipe.redis.proxy.tunnel.DefaultTunnel;
import com.ctrip.xpipe.redis.proxy.tunnel.state.FrontendClosed;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.nio.charset.Charset;

import static com.ctrip.xpipe.redis.proxy.handler.AbstractSessionNettyHandler.HIGH_WATER_MARK;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/**
 * @author chen.zhu
 * <p>
 * May 29, 2018
 */
public class AbstractSessionNettyHandlerTest extends AbstractNettyTest {

    @Mock
    private DefaultTunnel tunnel;

    @Mock
    private Session session;

    private AbstractSessionNettyHandler handler = new AbstractSessionNettyHandler() {

        @Override
        protected void setTunnelStateWhenSessionClosed() {
            tunnel.setState(new FrontendClosed((DefaultTunnel) tunnel));
        }
    };

    @Before
    public void beforeAbstractSessionNettyHandlerTest() {
        MockitoAnnotations.initMocks(this);
        handler.setSession(session);
        handler.setTunnel(tunnel);
    }

    @Test
    public void channelActive() {
        EmbeddedChannel channel = new EmbeddedChannel(handler);
        Assert.assertTrue(channel.isActive());
        Assert.assertEquals(HIGH_WATER_MARK, channel.config().getWriteBufferHighWaterMark());
    }

    @Test
    public void channelInactive() {
        EmbeddedChannel channel = new EmbeddedChannel(handler);
        channel.finish();
        Assert.assertFalse(channel.isActive());
        verify(tunnel).setState(new FrontendClosed(tunnel));
        verify(session).setSessionState(new SessionClosed(session));
    }

    class NettyTestExceptionHandler extends ChannelInboundHandlerAdapter {
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        }
    }

    @Test
    public void channelWritabilityChanged() {
        doNothing().when(session).onChannelNotWritable();
        EmbeddedChannel channel = new EmbeddedChannel(handler);
        channel.config().setWriteBufferHighWaterMark(32768 + 200);
        channel.config().setWriteBufferLowWaterMark(32768);
        int count = 0;
        while(count < 32768 + 200) {
            channel.write(Unpooled.copiedBuffer("hello world!".getBytes()));
            count += "hello world!".getBytes().length;
        }
        Assert.assertFalse(channel.isWritable());
        verify(session).onChannelNotWritable();

    }
}