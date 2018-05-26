package com.ctrip.xpipe.redis.core.proxy;

import com.ctrip.xpipe.redis.core.proxy.endpoint.ProxyEndpointManager;
import com.ctrip.xpipe.redis.core.proxy.endpoint.ProxyEndpointSelector;

/**
 * @author chen.zhu
 * <p>
 * May 13, 2018
 */
public interface ProxyEnabled {

    ProxyEndpointManager getProxyEndpointManager();

    void setProxyProtocol(ProxyProtocol protocol);

    ProxyProtocol getProxyProtocol();

    void setProxyEndpointSelector(ProxyEndpointSelector selector);

    ProxyEndpointSelector getProxyEndpointSelector();
}
