package com.ctrip.xpipe.redis.core.proxy.netty;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.proxy.ProxyEnabled;
import com.ctrip.xpipe.api.proxy.ProxyConnectProtocol;
import com.ctrip.xpipe.netty.commands.AsyncNettyClient;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.netty.commands.NettyClientHandler;
import com.ctrip.xpipe.netty.commands.NettyKeyedPoolClientFactory;
import com.ctrip.xpipe.redis.core.proxy.ProxyResourceManager;
import com.ctrip.xpipe.redis.core.proxy.ProxyedConnectionFactory;
import com.ctrip.xpipe.redis.core.proxy.connect.DefaultProxyedConnectionFactory;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author chen.zhu
 * <p>
 * Aug 05, 2018
 */
public class ProxyEnabledNettyKeyedPoolClientFactory extends NettyKeyedPoolClientFactory {

    private static final Logger logger = LoggerFactory.getLogger(ProxyEnabledNettyKeyedPoolClientFactory.class);

    private ProxyedConnectionFactory proxyedConnectionFactory;

    public ProxyEnabledNettyKeyedPoolClientFactory(ProxyResourceManager resourceManager) {
        this.proxyedConnectionFactory = new DefaultProxyedConnectionFactory(resourceManager);
    }

    @Override
    public PooledObject<NettyClient> makeObject(Endpoint key) throws Exception {
        if(!isProxyEnabled(key)) {
            return super.makeObject(key);
        }
        ProxyConnectProtocol protocol = ((ProxyEnabled) key).getProxyProtocol();
        ChannelFuture f = proxyedConnectionFactory.getProxyedConnectionChannelFuture(protocol, b);
        f.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                future.channel().writeAndFlush(protocol.output());
            }
        });
        NettyClient nettyClient = new AsyncNettyClient(f, key);
        f.channel().attr(NettyClientHandler.KEY_CLIENT).set(nettyClient);
        return new DefaultPooledObject<>(nettyClient);
    }

    private boolean isProxyEnabled(Endpoint key) {
        return key instanceof ProxyEnabled;
    }
}
