package com.ctrip.xpipe.redis.proxy;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.utils.FastThreadLocalThreadFactory;
import com.ctrip.xpipe.redis.proxy.config.ProxyConfig;
import com.ctrip.xpipe.redis.proxy.handler.InternalNetworkHandler;
import com.ctrip.xpipe.redis.proxy.handler.ProxyProtocolDecoder;
import com.ctrip.xpipe.redis.proxy.handler.ProxyProtocolHandler;
import com.ctrip.xpipe.redis.proxy.monitor.stats.PingStatsManager;
import com.ctrip.xpipe.redis.proxy.resource.ResourceManager;
import com.ctrip.xpipe.redis.proxy.tunnel.TunnelManager;
import com.ctrip.xpipe.utils.OsUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.net.InetSocketAddress;

/**
 * @author chen.zhu
 * <p>
 * May 09, 2018
 */
@Component
public class DefaultProxyServer implements ProxyServer {

    private Logger logger = LoggerFactory.getLogger(DefaultProxyServer.class);

    private static LoggingHandler loggingHandler = new LoggingHandler(LogLevel.DEBUG);

    @Autowired
    private ResourceManager resourceManager;

    @Autowired
    private ProxyConfig config;

    @Autowired
    private TunnelManager tunnelManager;

    @Autowired
    private PingStatsManager pingStatsManager;

    private ChannelFuture tcpFuture, tlsFuture;

    private static final int MEGA_BYTE = 1000 * 1000;

    public static final int WRITE_LOW_WATER_MARK = 10 * MEGA_BYTE;

    public static final int WRITE_HIGH_WATER_MARK = 5 * WRITE_LOW_WATER_MARK;

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
        logger.info("[startTcpServer] start with port: {}", config.frontendTcpPort());
        ServerBootstrap b = bootstrap("tcp").childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) throws Exception {

                ChannelPipeline p = ch.pipeline();
                p.addLast(new InternalNetworkHandler(config.getInternalNetworkPrefix()));
                p.addLast(loggingHandler);
                p.addLast(new ProxyProtocolDecoder(ProxyProtocolDecoder.DEFAULT_MAX_LENGTH));
                p.addLast(new ProxyProtocolHandler(tunnelManager, resourceManager, pingStatsManager));
            }
        });
        // port 80 bind only local address
        InetSocketAddress bindAddress = new InetSocketAddress(FoundationService.DEFAULT.getLocalIp(),
                config.frontendTcpPort());
        logger.info("[startTcpServer] bind socket: {}", bindAddress);
        tcpFuture = b.bind(bindAddress).sync();
    }

    private void startTlsServer() throws Exception {
        // Test Logic, no use for product
        int port = config.frontendTlsPort();
        if(port == -1) {
            return;
        }
        logger.info("[startTlsServer] start with port: {}", port);
        ServerBootstrap b = bootstrap("tls").childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) throws Exception {
                ChannelPipeline p = ch.pipeline();
                if(!config.noTlsNettyHandler()) {
                    p.addLast(resourceManager.getServerSslHandlerFactory().createSslHandler(ch));
                }
                p.addLast(loggingHandler);
                p.addLast(new ProxyProtocolDecoder(ProxyProtocolDecoder.DEFAULT_MAX_LENGTH));
                p.addLast(new ProxyProtocolHandler(tunnelManager, resourceManager, pingStatsManager));
            }
        });
        logger.info("[startTlsServer] bind tls port: {}", config.frontendTlsPort());
        tlsFuture = b.bind(config.frontendTlsPort()).sync();
    }

    private ServerBootstrap bootstrap(String prefix) {

        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(new NioEventLoopGroup(1, FastThreadLocalThreadFactory.create("boss-" + prefix)),
                new NioEventLoopGroup(Math.min(OsUtils.getCpuCount() * 2, 8), FastThreadLocalThreadFactory.create("worker-" + prefix)))
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .childOption(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(WRITE_LOW_WATER_MARK, WRITE_HIGH_WATER_MARK))
                .childOption(ChannelOption.RCVBUF_ALLOCATOR, new FixedRecvByteBufAllocator(config.getFixedRecvBufferSize()))
                .handler(loggingHandler);
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
        tunnelManager.removeAll();
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
    public DefaultProxyServer setResourceManager(ResourceManager resourceManager) {
        this.resourceManager = resourceManager;
        return this;
    }

    @VisibleForTesting
    public ProxyConfig getConfig() {
        return config;
    }

    @VisibleForTesting
    public TunnelManager getTunnelManager() {
        return this.tunnelManager;
    }

    @VisibleForTesting
    public DefaultProxyServer setPingStatsManager(PingStatsManager pingStatsManager) {
        this.pingStatsManager = pingStatsManager;
        return this;
    }

    @VisibleForTesting
    public PingStatsManager getPingStatsManager() {
        return pingStatsManager;
    }

    @VisibleForTesting
    public ResourceManager getResourceManager() {
        return resourceManager;
    }
}
