package com.ctrip.framework.xpipe.redis.proxy;

import com.ctrip.xpipe.api.proxy.ProxyConnectProtocol;
import com.ctrip.xpipe.proxy.ProxyEndpoint;
import com.ctrip.xpipe.redis.core.exception.NoResourceException;
import com.ctrip.xpipe.redis.core.proxy.endpoint.DefaultProxyEndpointManager;
import com.ctrip.xpipe.redis.core.proxy.endpoint.NaiveNextHopAlgorithm;
import com.ctrip.xpipe.redis.core.proxy.endpoint.ProxyEndpointManager;
import com.ctrip.xpipe.redis.core.proxy.endpoint.ProxyEndpointSelector;
import com.ctrip.xpipe.redis.core.proxy.resource.KeeperProxyResourceManager;
import io.netty.buffer.ByteBuf;

public class DefaultProxyResourceManager implements ProxyResourceManager {

    private ProxyConnectProtocol proxyConnectProtocol;

    private ProxyEndpointSelector proxyEndpointSelector;

    private ProxyEndpointManager endpointManager;

    public DefaultProxyResourceManager(ProxyConnectProtocol proxyConnectProtocol) {
        this.proxyConnectProtocol = proxyConnectProtocol;
        endpointManager = new DefaultProxyEndpointManager(()->2);
        KeeperProxyResourceManager proxyResourceManager = new KeeperProxyResourceManager(endpointManager, new NaiveNextHopAlgorithm());
        proxyEndpointSelector = proxyResourceManager.createProxyEndpointSelector(proxyConnectProtocol);
        start();
    }

    @Override
    public ByteBuf getProxyConnectProtocol() {
        return proxyConnectProtocol.output();
    }

    @Override
    public ProxyEndpoint nextHop() throws NoResourceException {
        return proxyEndpointSelector.nextHop();
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() throws Exception {
        endpointManager.stop();
    }
}
