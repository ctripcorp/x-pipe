package com.ctrip.xpipe.redis.core.proxy.endpoint;

import com.ctrip.xpipe.api.endpoint.Endpoint;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * May 04, 2018
 */
public interface NextJumpAlgorithm {

    ProxyEndpoint nextJump(List<ProxyEndpoint> endpoints);
}
