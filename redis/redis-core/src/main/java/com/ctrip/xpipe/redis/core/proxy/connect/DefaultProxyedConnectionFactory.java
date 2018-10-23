package com.ctrip.xpipe.redis.core.proxy.connect;

import com.ctrip.xpipe.api.proxy.ProxyConnectProtocol;
import com.ctrip.xpipe.proxy.ProxyEndpoint;
import com.ctrip.xpipe.redis.core.proxy.ProxyResourceManager;
import com.ctrip.xpipe.redis.core.proxy.ProxyedConnectionFactory;
import com.ctrip.xpipe.redis.core.proxy.endpoint.ProxyEndpointSelector;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;

/**
 * @author chen.zhu
 * <p>
 * Aug 15, 2018
 */
public class DefaultProxyedConnectionFactory implements ProxyedConnectionFactory {

    private ProxyResourceManager resourceManager;

    public DefaultProxyedConnectionFactory(ProxyResourceManager resourceManager) {
        this.resourceManager = resourceManager;
    }

    @Override
    public ChannelFuture getProxyedConnectionChannelFuture(ProxyConnectProtocol protocol, Bootstrap bootstrap) {
        ProxyEndpointSelector selector = resourceManager.createProxyEndpointSelector(protocol);
        ProxyEndpoint proxyEndpoint = selector.nextHop();
        return bootstrap.connect(proxyEndpoint.getHost(), proxyEndpoint.getPort());
    }
}
