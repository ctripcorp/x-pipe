package com.ctrip.xpipe.redis.core.proxy.endpoint;

import com.ctrip.xpipe.api.endpoint.Endpoint;

/**
 * @author chen.zhu
 * <p>
 * May 15, 2018
 */
public interface EndpointHealthChecker {

    boolean checkConnectivity(Endpoint endpoint);

}
