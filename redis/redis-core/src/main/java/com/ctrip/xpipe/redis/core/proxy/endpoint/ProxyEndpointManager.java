package com.ctrip.xpipe.redis.core.proxy.endpoint;

import com.ctrip.xpipe.api.lifecycle.Startable;
import com.ctrip.xpipe.api.lifecycle.Stoppable;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * May 10, 2018
 */
public interface ProxyEndpointManager extends Startable, Stoppable {

    List<ProxyEndpoint> getAvailableProxyEndpoints();

    List<ProxyEndpoint> getAllProxyEndpoints();

    ProxyEndpoint getNextJump(List<ProxyEndpoint> candidates);

    void setNextJumpAlgorithm(NextJumpAlgorithm algorithm);

}
