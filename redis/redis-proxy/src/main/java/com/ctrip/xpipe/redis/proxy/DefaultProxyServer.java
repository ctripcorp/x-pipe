package com.ctrip.xpipe.redis.proxy;

import com.ctrip.xpipe.redis.core.proxy.handler.NettySslHandlerFactory;
import com.ctrip.xpipe.redis.proxy.config.ProxyConfig;
import com.ctrip.xpipe.redis.proxy.handler.FrontendSessionNettyHandler;
import com.ctrip.xpipe.redis.proxy.handler.ProxyProtocolDecoder;
import com.ctrip.xpipe.redis.proxy.spring.Production;
import com.ctrip.xpipe.redis.proxy.tunnel.TunnelManager;
import com.ctrip.xpipe.utils.OsUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;

/**
 * @author chen.zhu
 * <p>
 * May 09, 2018
 */
@Component
public class DefaultProxyServer implements ProxyServer {

    @Autowired
    private ProxyConfig config;

    @Autowired
    private TunnelManager tunnelManager;

    @Resource(name = Production.SERVER_SSL_HANDLER_FACTORY)
    private NettySslHandlerFactory serverSslHandlerFactory;

    private ChannelFuture tcpFuture, tlsFuture;

    public static final int WRITE_HIGH_WATER_MARK = 8 * 1024 * 1024;

    public static final int WRITE_LOW_WATER_MARK = 2 * 1024 * 1024;

    public DefaultProxyServer() {
    }

    @PostConstruct
    public void startServer() throws Exception {
        start();
    }

    @PreDestroy
    public void stopServer() throws Exception {
        stop();
    }

    @Override
    public void start() throws Exception {
        startTcpServer();
        startTlsServer();
    }

    private void startTcpServer() throws Exception {
        ServerBootstrap b = bootstrap("tcp").childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) throws Exception {

                ChannelPipeline p = ch.pipeline();
                p.addLast(new LoggingHandler(LogLevel.DEBUG));
                p.addLast(new ProxyProtocolDecoder(ProxyProtocolDecoder.DEFAULT_MAX_LENGTH));
                p.addLast(new FrontendSessionNettyHandler(tunnelManager));
            }
        });

        tcpFuture = b.bind(config.frontendTcpPort()).sync();
    }

    private void startTlsServer() throws Exception {
        // Test Logic, no use for product
        int port = config.frontendTlsPort();
        if(port == -1) {
            return;
        }
        ServerBootstrap b = bootstrap("tls").childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) throws Exception {

                ChannelPipeline p = ch.pipeline();
                p.addLast(serverSslHandlerFactory.createSslHandler());
                p.addLast(new LoggingHandler(LogLevel.DEBUG));
                p.addLast(new ProxyProtocolDecoder(ProxyProtocolDecoder.DEFAULT_MAX_LENGTH));
                p.addLast(new FrontendSessionNettyHandler(tunnelManager));
            }
        });

        tlsFuture = b.bind(config.frontendTlsPort()).sync();
    }

    private ServerBootstrap bootstrap(String prefix) {

        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(new NioEventLoopGroup(1, XpipeThreadFactory.create("frontend-boss-" + prefix)),
                new NioEventLoopGroup(OsUtils.getCpuCount() * 2, XpipeThreadFactory.create("frontend-worker-" + prefix)))
                .channel(NioServerSocketChannel.class)
                .childOption(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, WRITE_HIGH_WATER_MARK)
                .childOption(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, WRITE_LOW_WATER_MARK)
                .handler(new LoggingHandler(LogLevel.DEBUG));
        return bootstrap;
    }

    @Override
    public void stop() {
        if(tcpFuture != null) {
            tcpFuture.channel().close();
        }
        if(tlsFuture != null) {
            tlsFuture.channel().close();
        }
    }


    /** Unit Test Use*/
    @VisibleForTesting
    public DefaultProxyServer setConfig(ProxyConfig config) {
        this.config = config;
        return this;
    }

    @VisibleForTesting
    public DefaultProxyServer setTunnelManager(TunnelManager tunnelManager) {
        this.tunnelManager = tunnelManager;
        return this;
    }

    @VisibleForTesting
    public DefaultProxyServer setServerSslHandlerFactory(NettySslHandlerFactory serverSslHandlerFactory) {
        this.serverSslHandlerFactory = serverSslHandlerFactory;
        return this;
    }

    @VisibleForTesting
    public ProxyConfig getConfig() {
        return config;
    }
}
