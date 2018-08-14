package com.ctrip.xpipe.redis.core.proxy.endpoint;

import com.ctrip.xpipe.api.lifecycle.Startable;
import com.ctrip.xpipe.api.lifecycle.Stoppable;
import com.ctrip.xpipe.proxy.ProxyEndpoint;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * May 10, 2018
 */

public interface ProxyEndpointManager extends Startable, Stoppable {

    List<ProxyEndpoint> getAvailableProxyEndpoints();

    List<ProxyEndpoint> getAllProxyEndpoints();

    void storeProxyEndpoints(List<ProxyEndpoint> endpoints);

}
