package com.ctrip.xpipe.redis.checker.healthcheck.impl;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;

/**
 * @author chen.zhu
 * <p>
 * Sep 03, 2018
 */
public interface HealthCheckEndpointFactory {

    Endpoint getOrCreateEndpoint(RedisMeta redisMeta);

    Endpoint getOrCreateEndpoint(HostPort hostPort);

    void updateRoutes();

    void remove(HostPort hostPort);
    
    
}
