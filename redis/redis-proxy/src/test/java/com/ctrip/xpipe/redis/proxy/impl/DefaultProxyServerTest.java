package com.ctrip.xpipe.redis.proxy.impl;

import com.ctrip.xpipe.redis.proxy.Tunnel;
import com.ctrip.xpipe.redis.proxy.handler.ProxyProtocolDecoder;
import com.ctrip.xpipe.redis.proxy.tunnel.TunnelManager;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * @author chen.zhu
 *         <p>
 *         May 10, 2018
 */
public class DefaultProxyServerTest {

    private static final Logger logger = LoggerFactory.getLogger(DefaultProxyServerTest.class);

    @Mock
    private TunnelManager tunnelManager;

    @Mock
    private Tunnel tunnel;

    @Before
    public void beforeDefaultProxyTest() {
        MockitoAnnotations.initMocks(this);
        when(tunnelManager.create(any(), any())).thenReturn(tunnel);
    }

    @Test
    public void handler() throws InterruptedException {

        int port = 8992;
        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(new NioEventLoopGroup(1), new NioEventLoopGroup(2))
                .channel(NioServerSocketChannel.class)
                .handler(new LoggingHandler(LogLevel.INFO))
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(new LoggingHandler(LogLevel.DEBUG));
                        p.addLast(new ProxyProtocolDecoder(1024));
                        p.addLast(new TestHandler());
                    }
                });

        bootstrap.bind(port).sync().channel().closeFuture().sync();
    }

    public class TestHandler extends ChannelDuplexHandler {
        @Override
        public void read(ChannelHandlerContext ctx) throws Exception {
            logger.info("handlers: {}", ctx.pipeline().toMap());
            super.read(ctx);
        }
    }

}