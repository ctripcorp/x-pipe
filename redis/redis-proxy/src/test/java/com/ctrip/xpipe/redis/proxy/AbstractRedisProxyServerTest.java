package com.ctrip.xpipe.redis.proxy;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.monitor.CatConfig;
import com.ctrip.xpipe.netty.NettySimpleMessageHandler;
import com.ctrip.xpipe.redis.core.proxy.DefaultProxyProtocolParser;
import com.ctrip.xpipe.redis.core.proxy.ProxyProtocol;
import com.ctrip.xpipe.redis.core.proxy.endpoint.*;
import com.ctrip.xpipe.redis.core.proxy.handler.NettyClientSslHandlerFactory;
import com.ctrip.xpipe.redis.core.proxy.handler.NettyServerSslHandlerFactory;
import com.ctrip.xpipe.redis.core.proxy.handler.NettySslHandlerFactory;
import com.ctrip.xpipe.redis.proxy.config.ProxyConfig;
import com.ctrip.xpipe.redis.proxy.tunnel.DefaultTunnelManager;
import com.ctrip.xpipe.redis.proxy.tunnel.TunnelManager;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import java.nio.charset.Charset;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author chen.zhu
 * <p>
 * May 12, 2018
 */

public class AbstractRedisProxyServerTest extends AbstractTest {

    private ProxyConfig config = new TestProxyConfig();

    private ProxyEndpointManager endpointManager = new DefaultProxyEndpointManager(
            ()->config.endpointHealthCheckIntervalSec());

    private TunnelManager tunnelManager = new DefaultTunnelManager();

    private NettySslHandlerFactory clientFactory = new NettyClientSslHandlerFactory(config);

    private NettySslHandlerFactory serverFactory = new NettyServerSslHandlerFactory(config);

    private DefaultProxyServer server;

    private ProxyProtocol proxyProtocol;

    private AtomicBoolean serverStarted = new AtomicBoolean(false);

    @BeforeClass
    public static void beforeAbstractRedisProxyServerTestClass() {
        System.setProperty(CatConfig.CAT_ENABLED_KEY, "false");
    }

    @Before
    public void beforeAbstractRedisProxyTest() {
        ((DefaultTunnelManager) tunnelManager).setEndpointManager(endpointManager);
        endpointManager.setNextJumpAlgorithm(new LocalNextJumpAlgorithm());
        ((DefaultProxyEndpointManager)endpointManager).setHealthChecker(new EndpointHealthChecker() {
            @Override
            public boolean checkConnectivity(Endpoint endpoint) {
                ProxyEndpoint proxyEndpoint = (ProxyEndpoint) endpoint;
                if(proxyEndpoint.isSslEnabled() || !proxyEndpoint.getHost().contains("127.0.0.1")) {
                    return false;
                }
                return true;
            }
        });
    }

    @After
    public void afterAbstractRedisProxyServerTest() throws Exception {
        if(server != null) {
            server.stop();
        }
    }

    public TunnelManager tunnelManager() {
        ((DefaultTunnelManager) tunnelManager).setEndpointManager(endpointManager);
        ((DefaultTunnelManager) tunnelManager).setConfig(config);
        ((DefaultTunnelManager) tunnelManager).setFactory(clientFactory);
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

    public ProxyProtocol protocol() {
        if(proxyProtocol == null) {
            String protocolStr = String.format("Proxy ROUTE %s,%s,%s %s,%s %s;PATH %s",
                    newProxyEndpoint(true, false), newProxyEndpoint(false, true), newProxyEndpoint(true, true),
                    newProxyEndpoint(true, true), newProxyEndpoint(false, true),
                    newProxyEndpoint(true, false),
                    newProxyEndpoint(true, false));
            proxyProtocol = new DefaultProxyProtocolParser().read(protocolStr);
        }
        return proxyProtocol;
    }

    public ProxyProtocol protocol(String route) {
        if(proxyProtocol == null) {
            String protocolStr = route;
            proxyProtocol = new DefaultProxyProtocolParser().read(protocolStr);
        }
        return proxyProtocol;
    }

    protected ProxyEndpoint newProxyEndpoint(boolean isLocal, boolean isSSL) {
        String local = "127.0.0.1", remote = String.format("10.3.%d.%d", randomInt(0, 255), randomInt(0, 255));
        String rawUri = String.format("%s://%s:%d",
                isSSL ? ProxyEndpoint.PROXY_SCHEME.PROXYTLS.name() : ProxyEndpoint.PROXY_SCHEME.PROXY.name(),
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
            server.setServerSslHandlerFactory(serverFactory());
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
        return b.connect("127.0.0.1", config.frontendPort());
    }

    public Channel frontChannel() throws Exception {
        ChannelFuture future = connect();
        return future.sync().channel();
    }

    public Tunnel tunnel() throws Exception {
        return tunnelManager.getOrCreate(frontChannel(), protocol());
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
}
