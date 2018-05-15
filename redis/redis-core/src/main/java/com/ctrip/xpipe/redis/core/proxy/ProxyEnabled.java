package com.ctrip.xpipe.redis.core.proxy;

import com.ctrip.xpipe.redis.core.proxy.endpoint.ProxyEndpointManager;

/**
 * @author chen.zhu
 * <p>
 * May 13, 2018
 */
public interface ProxyEnabled {

    ProxyEndpointManager getProxyEndpointManager();

    void setProxyProtocol(ProxyProtocol protocol);

    ProxyProtocol getProxyProtocol();
}
