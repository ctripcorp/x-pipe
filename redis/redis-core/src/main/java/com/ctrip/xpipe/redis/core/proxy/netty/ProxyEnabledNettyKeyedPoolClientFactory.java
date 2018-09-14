package com.ctrip.xpipe.redis.core.proxy.netty;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.proxy.ProxyEnabled;
import com.ctrip.xpipe.api.proxy.ProxyProtocol;
import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.netty.commands.DefaultNettyClient;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.netty.commands.NettyClientHandler;
import com.ctrip.xpipe.netty.commands.NettyKeyedPoolClientFactory;
import com.ctrip.xpipe.proxy.ProxyEndpoint;
import com.ctrip.xpipe.redis.core.proxy.ProxyResourceManager;
import com.ctrip.xpipe.redis.core.proxy.ProxyedConnectionFactory;
import com.ctrip.xpipe.redis.core.proxy.connect.DefaultProxyedConnectionFactory;
import com.ctrip.xpipe.redis.core.proxy.endpoint.ProxyEndpointSelector;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

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

    public ProxyEnabledNettyKeyedPoolClientFactory(ProxyResourceManager resourceManager, int eventLoopThreads) {
        super(eventLoopThreads);
    }

    @Override
    public PooledObject<NettyClient> makeObject(Endpoint key) throws Exception {
        if(!isProxyEnabled(key)) {
            return super.makeObject(key);
        }
        ProxyProtocol protocol = ((ProxyEnabled) key).getProxyProtocol();
        ChannelFuture f = proxyedConnectionFactory.getProxyedConnectionChannelFuture(protocol, b);
        f.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                future.channel().writeAndFlush(protocol.output());
            }
        });
        NettyClient nettyClient = new ProxyedNettyClient(f);
        f.channel().attr(NettyClientHandler.KEY_CLIENT).set(nettyClient);
        return new DefaultPooledObject<>(nettyClient);
    }

    private boolean isProxyEnabled(Endpoint key) {
        return key instanceof ProxyEnabled;
    }

    @Override
    public boolean validateObject(Endpoint key, PooledObject<NettyClient> p) {
        if(isProxyEnabled(key)) {
            return validateProxyedConnctionChannel(p.getObject().channel());
        }
        return super.validateObject(key, p);
    }

    private boolean validateProxyedConnctionChannel(Channel channel) {
        if(channel == null) {
            return false;
        }
        if(!channel.isOpen()) {
            return false;
        }
        return true;
    }
}
