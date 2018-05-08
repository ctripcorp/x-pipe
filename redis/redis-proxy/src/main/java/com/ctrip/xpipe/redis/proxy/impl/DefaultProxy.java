package com.ctrip.xpipe.redis.proxy.impl;

import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.redis.core.proxy.endpoint.ProxyEndpointManager;
import com.ctrip.xpipe.redis.proxy.Proxy;
import com.ctrip.xpipe.redis.proxy.config.ProxyConfig;
import com.ctrip.xpipe.redis.proxy.handler.ProxyProtocolHandler;
import com.ctrip.xpipe.redis.proxy.tunnel.TunnelManager;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.SelfSignedCertificate;

import javax.net.ssl.SSLException;
import java.security.cert.CertificateException;

/**
 * @author chen.zhu
 * <p>
 * May 09, 2018
 */
public class DefaultProxy extends AbstractLifecycle implements Proxy {

    private ProxyConfig config;

    private TunnelManager tunnelManager;

    private ProxyEndpointManager endpointManager;

    private final SslContext sslCtx;

    public DefaultProxy() throws CertificateException, SSLException {
        SelfSignedCertificate ssc = new SelfSignedCertificate();
        sslCtx = SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey()).build();
    }

    @Override
    public ChannelFuture startServer() {
        int port = config.frontendPort();
        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(new NioEventLoopGroup(1), new NioEventLoopGroup(config.workerEventLoops()))
                .handler(new LoggingHandler(LogLevel.INFO))
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(new LoggingHandler(LogLevel.DEBUG));
                        if(config.isSslEnabled()) {
                            p.addLast("ssl", sslCtx.newHandler(ch.alloc()));
                        }
                        p.addLast(new ProxyProtocolHandler(tunnelManager));
                    }
                });

        return bootstrap.bind(port);
    }
}
