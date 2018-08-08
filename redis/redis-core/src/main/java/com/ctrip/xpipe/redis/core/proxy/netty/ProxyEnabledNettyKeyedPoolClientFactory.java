package com.ctrip.xpipe.redis.core.proxy.netty;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.proxy.ProxyEnabled;
import com.ctrip.xpipe.api.proxy.ProxyProtocol;
import com.ctrip.xpipe.netty.commands.DefaultNettyClient;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.netty.commands.NettyClientHandler;
import com.ctrip.xpipe.netty.commands.NettyKeyedPoolClientFactory;
import com.ctrip.xpipe.proxy.ProxyEndpoint;
import com.ctrip.xpipe.redis.core.proxy.ProxyResourceManager;
import com.ctrip.xpipe.redis.core.proxy.endpoint.ProxyEndpointSelector;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * @author chen.zhu
 * <p>
 * Aug 05, 2018
 */
public class ProxyEnabledNettyKeyedPoolClientFactory extends NettyKeyedPoolClientFactory {

    private static final Logger logger = LoggerFactory.getLogger(ProxyEnabledNettyKeyedPoolClientFactory.class);

    private ProxyResourceManager resourceManager;

    public ProxyEnabledNettyKeyedPoolClientFactory(ProxyResourceManager resourceManager) {
        this.resourceManager = resourceManager;
    }

    public ProxyEnabledNettyKeyedPoolClientFactory(ProxyResourceManager resourceManager, int eventLoopThreads) {
        super(eventLoopThreads);
        this.resourceManager = resourceManager;
    }

    @Override
    public PooledObject<NettyClient> makeObject(Endpoint key) throws Exception {
        if(!isProxyEnabled(key)) {
            return super.makeObject(key);
        }
        ProxyProtocol protocol = ((ProxyEnabled) key).getProxyProtocol();
        ProxyEndpointSelector selector = resourceManager.createProxyEndpointSelector(protocol);
        ProxyEndpoint proxyEndpoint = selector.nextHop();
        ChannelFuture f = b.connect(proxyEndpoint.getHost(), proxyEndpoint.getPort());
        f.get(connectTimeoutMilli, TimeUnit.MILLISECONDS);
        Channel channel = f.channel();
        logger.debug("[makeObject]{}", channel);
        channel.writeAndFlush(protocol.output());
        NettyClient nettyClient = new DefaultNettyClient(channel);
        channel.attr(NettyClientHandler.KEY_CLIENT).set(nettyClient);
        return new DefaultPooledObject<NettyClient>(nettyClient);
    }

    private boolean isProxyEnabled(Endpoint key) {
        return key instanceof ProxyEnabled;
    }
}
