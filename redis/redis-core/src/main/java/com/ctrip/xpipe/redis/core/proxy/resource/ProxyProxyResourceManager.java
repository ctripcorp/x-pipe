package com.ctrip.xpipe.redis.core.proxy.resource;

import com.ctrip.xpipe.api.proxy.ProxyConnectProtocol;
import com.ctrip.xpipe.redis.core.proxy.ProxyResourceManager;
import com.ctrip.xpipe.redis.core.proxy.endpoint.*;

/**
 * @author chen.zhu
 * <p>
 * Jul 12, 2018
 */
public class ProxyProxyResourceManager implements ProxyResourceManager {

    private ProxyEndpointManager endpointManager;

    private NextHopAlgorithm algorithm;

    public ProxyProxyResourceManager(ProxyEndpointManager endpointManager, NextHopAlgorithm algorithm) {
        this.endpointManager = endpointManager;
        this.algorithm = algorithm;
    }

    @Override
    public ProxyEndpointSelector createProxyEndpointSelector(ProxyConnectProtocol protocol) {
        ProxyEndpointSelector selector = new DefaultProxyEndpointSelector(protocol.nextEndpoints(), endpointManager);
        selector.setNextHopAlgorithm(algorithm);
        selector.setSelectStrategy(new SelectOneCycle(selector));
        return selector;
    }
}
