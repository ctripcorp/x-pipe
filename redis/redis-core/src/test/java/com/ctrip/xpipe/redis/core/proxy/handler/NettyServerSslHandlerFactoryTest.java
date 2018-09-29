package com.ctrip.xpipe.redis.core.proxy.handler;

import io.netty.channel.socket.nio.NioSocketChannel;
import org.junit.Test;

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