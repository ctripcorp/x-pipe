package com.ctrip.xpipe.redis.core.proxy.endpoint;

import com.ctrip.xpipe.proxy.ProxyEndpoint;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * May 04, 2018
 */
public interface NextHopAlgorithm {

    ProxyEndpoint nextHop(List<ProxyEndpoint> endpoints);
}
