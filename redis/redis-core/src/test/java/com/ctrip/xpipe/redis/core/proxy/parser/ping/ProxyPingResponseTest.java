package com.ctrip.xpipe.redis.core.proxy.parser.ping;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.string.StringDecoder;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.junit.Assert.*;

/**
 * @author chen.zhu
 * <p>
 * Oct 18, 2018
 */
public class ProxyPingResponseTest {

    private static final Logger logger = LoggerFactory.getLogger(ProxyPingResponseTest.class);

    private ProxyPingResponse response;

    @Test
    public void testFormat() {
        response = new ProxyPingResponse("10.2.2.1", 6379);
        EmbeddedChannel channel = new EmbeddedChannel(new StringDecoder());
        channel.writeInbound(response.format());
        Assert.assertEquals("+PONG 10.2.2.1:6379\r\n", channel.readInbound());
    }

    @Test
    public void testGetPayload() {
        response = new ProxyPingResponse("10.2.2.1", 6379);
        Assert.assertEquals("PONG 10.2.2.1:6379", response.getPayload());
    }

    @Test
    public void testTryRead() {
        response = new ProxyPingResponse();
        ByteBuf buffer = Unpooled.copiedBuffer("+PONG 127.0.0.1:6379\r\n".getBytes());
        response.tryRead(buffer);
        String result = response.getPayload();
        logger.info("{}", result);
        Assert.assertEquals("PONG 127.0.0.1:6379", result);
    }
}