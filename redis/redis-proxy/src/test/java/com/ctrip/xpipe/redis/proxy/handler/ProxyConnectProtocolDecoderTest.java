package com.ctrip.xpipe.redis.proxy.handler;

import com.ctrip.xpipe.redis.proxy.AbstractNettyTest;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderException;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author chen.zhu
 * <p>
 * May 29, 2018
 */
public class ProxyConnectProtocolDecoderTest extends AbstractNettyTest {

    private ProxyProtocolDecoder decoder = new ProxyProtocolDecoder(128);

    private EmbeddedChannel channel = new EmbeddedChannel(decoder);

    @Test(expected = DecoderException.class)
    public void testDecodeWithWriteExhausted() throws Exception {
        int counter = 0;
        while(counter <= 128) {
            channel.writeInbound(Unpooled.copiedBuffer("Test".getBytes()));
            counter += "Test".getBytes().length;
        }

    }


    @Test
    public void testDecodeWithPositive() {
        ProxyProtocolDecoder decoder = new ProxyProtocolDecoder(1024);
        EmbeddedChannel channel = new EmbeddedChannel(decoder, new ListeningNettyHandler("TEST"));
        channel.writeInbound(Unpooled.copiedBuffer("+PROXY ROUTE TCP://127.0.0.1:6379 PROXYTCP://127.0.0.1:6380\r\nTEST".getBytes()));
    }

    @Test
    public void channelRead() {
        channel.writeInbound(Unpooled.copiedBuffer("+PROXY ROUTE TCP://127.0.0.1:6379 PROXYTCP://127.0.0.1:6380\r\n".getBytes()));
        Assert.assertTrue(decoder.isFinished());
    }

    @Test
    public void isSingleDecode() {
        Assert.assertTrue(decoder.isSingleDecode());
    }
}