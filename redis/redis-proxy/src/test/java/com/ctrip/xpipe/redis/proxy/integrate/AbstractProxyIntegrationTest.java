package com.ctrip.xpipe.redis.proxy.integrate;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.api.lifecycle.ComponentRegistry;
import com.ctrip.xpipe.redis.core.proxy.endpoint.DefaultProxyEndpointManager;
import com.ctrip.xpipe.redis.core.proxy.endpoint.EndpointHealthChecker;
import com.ctrip.xpipe.redis.core.proxy.endpoint.NaiveNextHopAlgorithm;
import com.ctrip.xpipe.redis.core.proxy.handler.NettyClientSslHandlerFactory;
import com.ctrip.xpipe.redis.core.proxy.handler.NettyServerSslHandlerFactory;
import com.ctrip.xpipe.redis.core.proxy.resource.ProxyProxyResourceManager;
import com.ctrip.xpipe.redis.proxy.AbstractNettyTest;
import com.ctrip.xpipe.redis.proxy.AbstractRedisProxyServerTest;
import com.ctrip.xpipe.redis.proxy.DefaultProxyServer;
import com.ctrip.xpipe.redis.proxy.TestProxyConfig;
import com.ctrip.xpipe.redis.proxy.controller.ComponentRegistryHolder;
import com.ctrip.xpipe.redis.proxy.tunnel.DefaultTunnelManager;
import com.ctrip.xpipe.utils.OsUtils;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

import java.nio.charset.Charset;
import java.util.concurrent.atomic.AtomicReference;

import static com.ctrip.xpipe.redis.proxy.spring.Production.*;
import static io.netty.handler.codec.ByteToMessageDecoder.MERGE_CUMULATOR;
import static org.mockito.Mockito.*;

/**
 * @author chen.zhu
 * <p>
 * May 23, 2018
 */
public class AbstractProxyIntegrationTest extends AbstractTest {

    protected void prepare(DefaultProxyServer server) {
        sameStuff(server);
    }

    protected void prepareTLS(DefaultProxyServer server) {
        sameStuff(server);
    }

    private void sameStuff(DefaultProxyServer server) {
        FoundationService service = FoundationService.DEFAULT;
        service = spy(FoundationService.DEFAULT);
        doReturn("127.0.0.1").when(service).getLocalIp();
        server.setServerSslHandlerFactory(new NettyServerSslHandlerFactory(new TestProxyConfig()));
        DefaultProxyEndpointManager endpointManager = new DefaultProxyEndpointManager(()-> 180);
        endpointManager.setHealthChecker(new EndpointHealthChecker() {
            @Override
            public boolean checkConnectivity(Endpoint endpoint) {
                return true;
            }
        });
        server.setTunnelManager(new DefaultTunnelManager()
                .setConfig(server.getConfig()).setProxyResourceManager(
                        new ProxyProxyResourceManager(endpointManager, new NaiveNextHopAlgorithm())));

        ComponentRegistry registry = mock(ComponentRegistry.class);
        when(registry.getComponent(CLIENT_SSL_HANDLER_FACTORY)).thenReturn(new NettyClientSslHandlerFactory(new TestProxyConfig()));
        when(registry.getComponent(SERVER_SSL_HANDLER_FACTORY)).thenReturn(new NettyServerSslHandlerFactory(new TestProxyConfig()));
        when(registry.getComponent(BACKEND_EVENTLOOP_GROUP)).thenReturn(new NioEventLoopGroup(OsUtils.getCpuCount()));
        ComponentRegistryHolder.initializeRegistry(registry);
    }

    protected String generateSequncialString(int length) {
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < length; i++) {
            sb.append(i).append(' ');
            if(i != 0 && (i % 10) == 0) {
                sb.append("\r\n");
            }
        }
        return sb.toString();
    }

    protected void write(ChannelFuture future, String sendout) {
        future.channel().writeAndFlush(UnpooledByteBufAllocator.DEFAULT.buffer().writeBytes(sendout.getBytes()));
    }

    public ChannelFuture startListenServer(int port) {
        return serverBootstrap().childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                ch.pipeline().addLast(new AbstractProxyIntegrationTest.ListeningHandler());
            }
        }).bind(port);
    }

    public ChannelFuture startReceiveServer(int port, AtomicReference<ByteBuf> receivedBuf) {
        return serverBootstrap().childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                ch.pipeline().addLast(new AbstractProxyIntegrationTest.ReceiveHandler(receivedBuf));
            }
        }).bind(port);
    }

    class ListeningHandler extends ChannelInboundHandlerAdapter {
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            ByteBuf byteBuf = (ByteBuf) msg;
            logger.info("[ListeningHandler][receive] {}", byteBuf.toString(Charset.defaultCharset()));
            super.channelRead(ctx, msg);
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            logger.info("[ListeningHandler][channel in-active");
            super.channelInactive(ctx);
        }

        @Override
        public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
            logger.info("[ListeningHandler][channel un-register");
            super.channelUnregistered(ctx);
        }
    }

    class ReceiveHandler extends ChannelInboundHandlerAdapter {

        private ByteToMessageDecoder.Cumulator cumulator = MERGE_CUMULATOR;

        private ByteBuf cumulation;

        private AtomicReference<ByteBuf> buffer;

        public ReceiveHandler(AtomicReference<ByteBuf> buffer) {
            this.buffer = buffer;
            cumulation = buffer.get();
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            ByteBuf byteBuf = (ByteBuf) msg;
            byteBuf.retain();
            cumulator.cumulate(ctx.channel().alloc(), cumulation, byteBuf);
            buffer.set(cumulation);
            super.channelRead(ctx, msg);
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            logger.info("[ReceiveHandler][channel in-active");
            buffer.set(cumulation);
            super.channelInactive(ctx);
        }

        @Override
        public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
            logger.info("[ReceiveHandler][channel un-register");
            super.channelUnregistered(ctx);
        }
    }

    public Bootstrap clientBootstrap() {
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.channel(NioSocketChannel.class)
                .group(new NioEventLoopGroup(1))
                .option(ChannelOption.TCP_NODELAY, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(new LoggingHandler(LogLevel.DEBUG));
                    }
                });
        return bootstrap;
    }

    public ServerBootstrap serverBootstrap() {
        ServerBootstrap b = new ServerBootstrap();
        b.channel(NioServerSocketChannel.class)
                .group(new NioEventLoopGroup(1), new NioEventLoopGroup(1))
                .option(ChannelOption.TCP_NODELAY, true)
                .handler(new LoggingHandler(LogLevel.DEBUG));
        return b;
    }
}
