package com.ctrip.xpipe.redis.core.proxy.endpoint;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * May 10, 2018
 */
public interface ProxyEndpointManager {

    List<ProxyEndpoint> getAvailableProxyEndpoints();

    List<ProxyEndpoint> getAllProxyEndpoints();

    ProxyEndpoint getNextJump(List<ProxyEndpoint> candidates);

    void setNextJumpAlgorithm(NextJumpAlgorithm algorithm);

    boolean checkConnectivity(ProxyEndpoint endpoint);
}
