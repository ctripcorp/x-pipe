package com.ctrip.xpipe.redis.proxy.resource;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.netty.NettySimpleMessageHandler;
import com.ctrip.xpipe.netty.commands.AsyncNettyClient;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.netty.commands.NettyClientHandler;
import com.ctrip.xpipe.netty.commands.NettyKeyedPoolClientFactory;
import com.ctrip.xpipe.proxy.ProxyEndpoint;
import com.ctrip.xpipe.utils.OsUtils;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LoggingHandler;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

/**
 * @author chen.zhu
 * <p>
 * Oct 31, 2018
 */
public class SslEnabledNettyClientFactory extends NettyKeyedPoolClientFactory {

    private ResourceManager resourceManager;

    public SslEnabledNettyClientFactory(ResourceManager resourceManager) {
        this(Math.min(4, OsUtils.getCpuCount()), resourceManager);
    }

    public SslEnabledNettyClientFactory(int eventLoopThreads, ResourceManager resourceManager) {
        super(eventLoopThreads);
        this.resourceManager = resourceManager;
    }

    @Override
    protected void initBootstrap() {
        b.group(eventLoopGroup).channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeoutMilli);
    }

    @Override
    public PooledObject<NettyClient> makeObject(Endpoint key) {
        ProxyEndpoint endpoint = (ProxyEndpoint) key;
        ChannelFuture f = getBootstrap(endpoint).connect(key.getHost(), key.getPort());
        NettyClient nettyClient = new AsyncNettyClient(f, key);
        f.channel().attr(NettyClientHandler.KEY_CLIENT).set(nettyClient);
        return new DefaultPooledObject<>(nettyClient);
    }

    private Bootstrap getBootstrap(ProxyEndpoint endpoint) {
        b.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) {
                ChannelPipeline p = ch.pipeline();
                if(endpoint.isSslEnabled()) {
                    p.addLast(resourceManager.getClientSslHandlerFactory().createSslHandler(ch));
                }
                p.addLast(new LoggingHandler());
                p.addLast(new NettySimpleMessageHandler());
                p.addLast(new NettyClientHandler());
            }
        });
        return b;
    }
}
