package com.ctrip.xpipe.redis.console.health;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;

/**
 * @author chen.zhu
 * <p>
 * Aug 21, 2018
 */
public interface HealthCheckEndpointManager {

    HealthCheckEndpoint getOrCreate(RedisMeta redisMeta);

    HealthCheckEndpoint getOrCreate(HostPort hostPort);
}
