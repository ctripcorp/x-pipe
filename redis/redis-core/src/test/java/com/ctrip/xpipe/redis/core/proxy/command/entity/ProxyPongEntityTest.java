package com.ctrip.xpipe.redis.core.proxy.command.entity;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.netty.ByteBufUtils;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ProxyPongEntityTest extends AbstractTest {

    @Test
    public void testOutput() {
        HostPort direct = localHostport(randomPort()), real = localHostport(randomPort());
        long rtt = 200;
        ProxyPongEntity pong = new ProxyPongEntity(direct);
        Assert.assertEquals(Unpooled.copiedBuffer(("+PROXY PONG " + direct.toString() + "\r\n").getBytes()), pong.output());
        logger.info("[output] {}", ByteBufUtils.readToString(pong.output()));

        pong = new ProxyPongEntity(direct, real, rtt);
        String expected = String.format("+PROXY PONG %s %s %d\r\n", direct.toString(), real.toString(), rtt);
        Assert.assertEquals(Unpooled.copiedBuffer(expected.getBytes()), pong.output());
        logger.info("[output] {}", ByteBufUtils.readToString(pong.output()));
    }
}