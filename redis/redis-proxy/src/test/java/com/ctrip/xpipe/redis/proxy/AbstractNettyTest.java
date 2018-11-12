package com.ctrip.xpipe.redis.proxy;

import com.ctrip.xpipe.api.proxy.ProxyConnectProtocol;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.string.StringDecoder;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;

/**
 * @author chen.zhu
 * <p>
 * May 29, 2018
 */
public class AbstractNettyTest {

    protected static final Logger logger = LoggerFactory.getLogger(AbstractNettyTest.class);

    private Channel channel = new EmbeddedChannel();

    public Channel sameChannel() {
        return channel;
    }

    public Channel normalChannel() {
        return new EmbeddedChannel(new StringDecoder(Charset.defaultCharset()));
    }

    protected class ListeningNettyHandler extends ChannelInboundHandlerAdapter {

        String expected;

        public ListeningNettyHandler(String expected) {
            this.expected = expected;
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            if(msg instanceof ProxyConnectProtocol) {
                logger.info("{}", ((ProxyConnectProtocol) msg).getContent());
            }
            if(msg instanceof ByteBuf) {
                Assert.assertEquals(expected, ((ByteBuf) msg).toString(Charset.defaultCharset()));
                ((ByteBuf) msg).release();
            }
        }
    }
}
