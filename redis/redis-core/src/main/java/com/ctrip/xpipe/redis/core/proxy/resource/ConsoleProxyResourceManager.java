package com.ctrip.xpipe.redis.core.proxy.resource;

import com.ctrip.xpipe.api.proxy.ProxyConnectProtocol;
import com.ctrip.xpipe.redis.core.proxy.ProxyResourceManager;
import com.ctrip.xpipe.redis.core.proxy.endpoint.*;

/**
 * @author chen.zhu
 * <p>
 * Aug 06, 2018
 */
public class ConsoleProxyResourceManager implements ProxyResourceManager {

    private ProxyEndpointManager endpointManager;

    private NextHopAlgorithm algorithm;

    public ConsoleProxyResourceManager(ProxyEndpointManager endpointManager, NextHopAlgorithm algorithm) {
        this.endpointManager = endpointManager;
        this.algorithm = algorithm;
    }

    @Override
    public ProxyEndpointSelector createProxyEndpointSelector(ProxyConnectProtocol protocol) {
        ProxyEndpointSelector selector = new DefaultProxyEndpointSelector(protocol.nextEndpoints(), endpointManager);
        selector.setNextHopAlgorithm(algorithm);
        selector.setSelectStrategy(new SelectNTimes(selector, SelectNTimes.INFINITE));
        return selector;
    }
}
