package com.ctrip.xpipe.redis.proxy.integrate;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.proxy.ProxyEndpoint;
import com.ctrip.xpipe.redis.core.proxy.endpoint.DefaultProxyEndpointManager;
import com.ctrip.xpipe.redis.core.proxy.endpoint.EndpointHealthChecker;
import com.ctrip.xpipe.redis.core.proxy.endpoint.ProxyEndpointHealthChecker;
import com.ctrip.xpipe.redis.proxy.DefaultProxyServer;
import com.ctrip.xpipe.redis.proxy.ProxyServer;
import com.ctrip.xpipe.redis.proxy.Session;
import com.ctrip.xpipe.redis.proxy.TestProxyConfig;
import com.ctrip.xpipe.redis.proxy.monitor.DefaultTunnelMonitorManager;
import com.ctrip.xpipe.redis.proxy.monitor.stats.impl.DefaultPingStatsManager;
import com.ctrip.xpipe.redis.proxy.resource.TestResourceManager;
import com.ctrip.xpipe.redis.proxy.session.BackendSession;
import com.ctrip.xpipe.redis.proxy.session.FrontendSession;
import com.ctrip.xpipe.redis.proxy.tunnel.DefaultTunnelManager;
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
import org.junit.BeforeClass;

import java.nio.charset.Charset;
import java.util.concurrent.atomic.AtomicReference;

import static com.ctrip.xpipe.redis.proxy.monitor.session.DefaultSessionMonitor.SESSION_MONITOR_SHOULD_START;
import static io.netty.handler.codec.ByteToMessageDecoder.MERGE_CUMULATOR;
import static org.mockito.Mockito.*;

/**
 * @author chen.zhu
 * <p>
 * May 23, 2018
 */
public class AbstractProxyIntegrationTest extends AbstractTest {

    public static final int FIRST_PROXY_TCP_PORT = 8992, SEC_PROXY_TCP_PORT = 8993;

    public static final int FIRST_PROXY_TLS_PORT = 1443, SEC_PROXY_TLS_PORT = 2443;

    @BeforeClass
    public static void beforeAbstractProxyIntegrationTest() {
        System.setProperty(SESSION_MONITOR_SHOULD_START, "false");
    }

    protected void prepare(DefaultProxyServer server) {
        sameStuff(server);
    }

    protected void prepareTLS(DefaultProxyServer server) {
        sameStuff(server);
    }

    protected void sameStuff(DefaultProxyServer server) {
        FoundationService service = FoundationService.DEFAULT;
        service = spy(FoundationService.DEFAULT);
        doReturn("127.0.0.1").when(service).getLocalIp();
        DefaultProxyEndpointManager endpointManager = new DefaultProxyEndpointManager(()-> 180);
        endpointManager.setHealthChecker(new ProxyEndpointHealthChecker() {
            @Override
            public boolean checkConnectivity(ProxyEndpoint endpoint) {
                return true;
            }
        });
        TestResourceManager resourceManager = new TestResourceManager();
        resourceManager.setEndpointManager(endpointManager);
        resourceManager.setConfig(server.getConfig());
        server.setTunnelManager(new DefaultTunnelManager()
                .setConfig(server.getConfig()).setProxyResourceManager(resourceManager)
                .setTunnelMonitorManager(new DefaultTunnelMonitorManager(resourceManager)));
        server.setResourceManager(resourceManager);
        if(server.getPingStatsManager() != null) {
            ((DefaultPingStatsManager)server.getPingStatsManager()).setResourceManager(resourceManager)
                    .setEndpointManager(endpointManager);
        }
    }

    protected DefaultProxyServer startFirstProxy() throws Exception {
        // uncomment disable netty bytebuf test
//        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.DISABLED);
        DefaultProxyServer server = new DefaultProxyServer().setConfig(new TestProxyConfig()
                .setFrontendTcpPort(FIRST_PROXY_TCP_PORT).setFrontendTlsPort(FIRST_PROXY_TLS_PORT).setStartMonitor(false))
                .setPingStatsManager(new DefaultPingStatsManager());
        prepare(server);
        ((TestProxyConfig)server.getResourceManager().getProxyConfig()).setStartMonitor(false);
        ((DefaultPingStatsManager)server.getPingStatsManager()).postConstruct();
        server.start();
        return server;
    }


    protected void startSecondaryProxy() throws Exception {
        DefaultProxyServer server = new DefaultProxyServer().setConfig(new TestProxyConfig()
                .setFrontendTcpPort(SEC_PROXY_TCP_PORT).setFrontendTlsPort(SEC_PROXY_TLS_PORT).setStartMonitor(false))
                .setPingStatsManager(new DefaultPingStatsManager());
        prepare(server);
        ((TestProxyConfig)server.getResourceManager().getProxyConfig()).setStartMonitor(false).startMonitor();
        ((DefaultPingStatsManager)server.getPingStatsManager()).postConstruct();
        server.start();
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
        logger.info("[write] {}", sendout);
        future.channel().writeAndFlush(UnpooledByteBufAllocator.DEFAULT.buffer().writeBytes(sendout.getBytes()))
                .addListener(writeFuture -> {
                    if (writeFuture.isSuccess()) {
                        logger.info("[write][success] {}", sendout);
                    } else {
                        logger.info("[write][fail]", writeFuture.cause());
                    }
                });
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

    protected static Session mockSession() {
        return mock(Session.class);
    }

    protected static FrontendSession mockFrontendSession() {
        return mock(FrontendSession.class);
    }

    protected static BackendSession mockBackendSession() {
        return mock(BackendSession.class);
    }

    protected static Channel mockChannel() {
        return mock(Channel.class);
    }
}
