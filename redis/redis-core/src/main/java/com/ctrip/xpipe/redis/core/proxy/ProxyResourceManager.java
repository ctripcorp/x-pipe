package com.ctrip.xpipe.redis.core.proxy;

import com.ctrip.xpipe.api.proxy.ProxyConnectProtocol;
import com.ctrip.xpipe.redis.core.proxy.endpoint.ProxyEndpointSelector;

/**
 * @author chen.zhu
 * <p>
 * Jul 12, 2018
 */
public interface ProxyResourceManager {

    ProxyEndpointSelector createProxyEndpointSelector(ProxyConnectProtocol protocol);
}
