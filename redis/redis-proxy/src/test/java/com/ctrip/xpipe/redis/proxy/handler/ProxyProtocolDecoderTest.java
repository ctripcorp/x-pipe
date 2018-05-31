package com.ctrip.xpipe.redis.proxy.handler;

import com.ctrip.xpipe.redis.proxy.AbstractNettyTest;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author chen.zhu
 * <p>
 * May 29, 2018
 */
public class ProxyProtocolDecoderTest extends AbstractNettyTest {

    private ProxyProtocolDecoder decoder = new ProxyProtocolDecoder(128);

    private EmbeddedChannel channel = new EmbeddedChannel(decoder);

    @Test
    public void testDecodeWithWriteExhausted() throws Exception {
        int counter = 0;
        while(counter <= 128) {
            channel.writeInbound(Unpooled.copiedBuffer("Test".getBytes()));
            counter += "Test".getBytes().length;
        }
        channel.checkException();
    }


    @Test
    public void testDecodeWithPositive() {
        ProxyProtocolDecoder decoder = new ProxyProtocolDecoder(1024);
        EmbeddedChannel channel = new EmbeddedChannel(decoder, new ListeningNettyHandler("TEST"));
        channel.writeInbound(Unpooled.copiedBuffer("+PROXY ROUTE TCP://127.0.0.1:6379 PROXY://127.0.0.1:6380\r\nTEST".getBytes()));
    }

    @Test
    public void channelRead() {
        channel.writeInbound(Unpooled.copiedBuffer("+PROXY ROUTE TCP://127.0.0.1:6379 PROXY://127.0.0.1:6380\r\n".getBytes()));
        Assert.assertTrue(decoder.isFinished());
    }

    @Test
    public void isSingleDecode() {
        Assert.assertTrue(decoder.isSingleDecode());
    }
}