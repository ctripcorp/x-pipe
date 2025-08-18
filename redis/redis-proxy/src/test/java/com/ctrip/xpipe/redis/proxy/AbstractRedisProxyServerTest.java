package com.ctrip.xpipe.redis.proxy;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.lifecycle.ComponentRegistry;
import com.ctrip.xpipe.api.proxy.ProxyConnectProtocol;
import com.ctrip.xpipe.monitor.CatConfig;
import com.ctrip.xpipe.proxy.ProxyEndpoint;
import com.ctrip.xpipe.redis.core.proxy.endpoint.*;
import com.ctrip.xpipe.redis.core.proxy.handler.NettyClientSslHandlerFactory;
import com.ctrip.xpipe.redis.core.proxy.handler.NettyServerSslHandlerFactory;
import com.ctrip.xpipe.redis.core.proxy.handler.NettySslHandlerFactory;
import com.ctrip.xpipe.redis.core.proxy.parser.DefaultProxyConnectProtocolParser;
import com.ctrip.xpipe.redis.proxy.config.ProxyConfig;
import com.ctrip.xpipe.redis.proxy.controller.ComponentRegistryHolder;
import com.ctrip.xpipe.redis.proxy.monitor.DefaultTunnelMonitorManager;
import com.ctrip.xpipe.redis.proxy.monitor.session.DefaultSessionMonitor;
import com.ctrip.xpipe.redis.proxy.resource.ProxyRelatedResourceManager;
import com.ctrip.xpipe.redis.proxy.resource.ResourceManager;
import com.ctrip.xpipe.redis.proxy.resource.TestResourceManager;
import com.ctrip.xpipe.redis.proxy.session.BackendSession;
import com.ctrip.xpipe.redis.proxy.session.DefaultBackendSession;
import com.ctrip.xpipe.redis.proxy.session.DefaultFrontendSession;
import com.ctrip.xpipe.redis.proxy.session.FrontendSession;
import com.ctrip.xpipe.redis.proxy.tunnel.DefaultTunnel;
import com.ctrip.xpipe.redis.proxy.tunnel.DefaultTunnelManager;
import com.ctrip.xpipe.redis.proxy.tunnel.TunnelManager;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.mockito.MockitoAnnotations;

import java.nio.charset.Charset;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.ctrip.xpipe.redis.proxy.spring.Production.*;
import static io.netty.handler.codec.ByteToMessageDecoder.MERGE_CUMULATOR;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * @author chen.zhu
 * <p>
 * May 12, 2018
 */

public class AbstractRedisProxyServerTest extends AbstractTest {

    private ProxyConfig config = new TestProxyConfig();

    private ProxyEndpointManager endpointManager = new DefaultProxyEndpointManager(
            ()->config.endpointHealthCheckIntervalSec());

    private TunnelManager tunnelManager;

    private ResourceManager resourceManager = new TestResourceManager();

    private NextHopAlgorithm algorithm = new LocalNextHopAlgorithm();

    private NettySslHandlerFactory clientFactory = new NettyClientSslHandlerFactory(config);

    private NettySslHandlerFactory serverFactory = new NettyServerSslHandlerFactory(config);

    private DefaultProxyServer server;

    private ProxyConnectProtocol proxyConnectProtocol;

    private AtomicBoolean serverStarted = new AtomicBoolean(false);

    private DefaultTunnel tunnel;

    protected TestResourceManager proxyResourceManager = new TestResourceManager();

    @BeforeClass
    public static void beforeAbstractRedisProxyServerTestClass() {
        System.setProperty(CatConfig.CAT_ENABLED_KEY, "false");
        System.setProperty(DefaultSessionMonitor.SESSION_MONITOR_SHOULD_START, "false");
    }

    @Before
    public void beforeAbstractRedisProxyTest() {
        MockitoAnnotations.initMocks(this);
        ((DefaultProxyEndpointManager)endpointManager).setHealthChecker(new ProxyEndpointHealthChecker() {
            @Override
            public boolean checkConnectivity(ProxyEndpoint endpoint) {
                return true;
            }

            @Override
            public boolean resetIfNeed(ProxyEndpoint endpoint) {
                return false;
            }
        });
        tunnelManager = new DefaultTunnelManager();
        tunnelManager = spy(tunnelManager);

        ComponentRegistry registry = mock(ComponentRegistry.class);
        when(registry.getComponent(CLIENT_SSL_HANDLER_FACTORY))
                .thenReturn(new NettyClientSslHandlerFactory(new TestProxyConfig()));
        when(registry.getComponent(SERVER_SSL_HANDLER_FACTORY))
                .thenReturn(new NettyServerSslHandlerFactory(new TestProxyConfig()));
        ComponentRegistryHolder.initializeRegistry(registry);

        tunnel =  new DefaultTunnel(new EmbeddedChannel(), protocol(), new TestProxyConfig(),
                resourceManager, new DefaultTunnelMonitorManager(resourceManager), scheduled);
        tunnel = spy(tunnel);
        doReturn(tunnel).when(tunnelManager).create(any(), any());

        BackendSession backend = new DefaultBackendSession(tunnel, new NioEventLoopGroup(1), 3000, mock(ProxyRelatedResourceManager.class));
        doReturn(backend).when(tunnel).backend();

        FrontendSession frontend = new DefaultFrontendSession(tunnel, new EmbeddedChannel(), 30000);
        doReturn(frontend).when(tunnel).frontend();
    }

    @After
    public void afterAbstractRedisProxyServerTest() throws Exception {
        if(server != null) {
            server.stop();
        }
    }

    public TunnelManager tunnelManager() {
        ((DefaultTunnelManager) tunnelManager).setConfig(config);
        return tunnelManager;
    }

    public ProxyEndpointManager endpointManager() {
        return endpointManager;
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

    public ProxyConnectProtocol protocol() {
        if(proxyConnectProtocol == null) {
            String protocolStr = String.format("Proxy ROUTE %s,%s,%s %s,%s %s;FORWARD_FOR %s",
                    newProxyEndpoint(true, false), newProxyEndpoint(false, true), newProxyEndpoint(true, true),
                    newProxyEndpoint(true, true), newProxyEndpoint(false, true),
                    newProxyEndpoint(true, false),
                    newProxyEndpoint(true, false));
            proxyConnectProtocol = new DefaultProxyConnectProtocolParser().read(protocolStr);
        }
        return proxyConnectProtocol;
    }

    public ProxyConnectProtocol protocol(String route) {
        if(proxyConnectProtocol == null) {
            String protocolStr = route;
            proxyConnectProtocol = new DefaultProxyConnectProtocolParser().read(protocolStr);
        }
        return proxyConnectProtocol;
    }

    protected ProxyEndpoint newProxyEndpoint(boolean isLocal, boolean isSSL) {
        String local = "127.0.0.1", remote = String.format("10.3.%d.%d", randomInt(0, 255), randomInt(0, 255));
        String rawUri = String.format("%s://%s:%d",
                isSSL ? ProxyEndpoint.PROXY_SCHEME.TLS.name() : ProxyEndpoint.PROXY_SCHEME.PROXYTCP.name(),
                isLocal ? local : remote,
                randomPort());

        return new DefaultProxyEndpoint(rawUri);
    }

    public ProxyConfig config() {
        return config;
    }

    public NettySslHandlerFactory clientFactory() {
        return clientFactory;
    }

    public NettySslHandlerFactory serverFactory() {
        return serverFactory;
    }

    public void startServer() throws Exception {
        if(serverStarted.compareAndSet(false, true)) {
            server = new DefaultProxyServer();
            server.setConfig(config());
            server.setTunnelManager(tunnelManager());
            server.start();
        }
    }

    public ChannelFuture connect() throws Exception {

        startServer();
        Bootstrap b = new Bootstrap();
        b.group(new NioEventLoopGroup(1))
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline p = ch.pipeline();

                        p.addLast(new LoggingHandler(LogLevel.DEBUG));
                    }
                });
        return b.connect("127.0.0.1", config.frontendTcpPort());
    }

    public Channel frontChannel() throws Exception {
        ChannelFuture future = connect();
        return future.sync().channel();
    }

    public Tunnel tunnel() throws Exception {
        return tunnelManager.create(new EmbeddedChannel(), protocol());
    }

    public Session frontend() throws Exception {
        return tunnel().frontend();
    }

    public Session backend() throws Exception {
        return tunnel().backend();
    }

    public ChannelFuture startListenServer(int port) {
        return serverBootstrap().childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                ch.pipeline().addLast(new ListeningHandler());
            }
        }).bind(port);
    }

    public ChannelFuture startReceiveServer(int port, AtomicReference<ByteBuf> receivedBuf) {
        return serverBootstrap().childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                ch.pipeline().addLast(new ReceiveHandler(receivedBuf));
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
//            logger.info("[channelRead] {}", byteBuf.toString(Charset.defaultCharset()));
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
}
