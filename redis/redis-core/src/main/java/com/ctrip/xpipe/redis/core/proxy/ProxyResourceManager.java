package com.ctrip.xpipe.redis.core.proxy;

import com.ctrip.xpipe.redis.core.proxy.endpoint.ProxyEndpoint;
import com.ctrip.xpipe.redis.core.proxy.endpoint.ProxyEndpointSelector;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Jul 12, 2018
 */
public interface ProxyResourceManager {

    ProxyEndpointSelector createProxyEndpointSelector(ProxyProtocol protocol);
}
