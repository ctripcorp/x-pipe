package com.ctrip.xpipe.redis.core.proxy.handler;

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author chen.zhu
 * <p>
 * May 25, 2018
 */
public class NettyServerSslHandlerFactoryTest {

    private NettyServerSslHandlerFactory factory = new NettyServerSslHandlerFactory(new FakeTLSConfig());

    @Test
    public void testCreateSslHandler() {
        factory.createSslHandler(new NioSocketChannel());
    }
}