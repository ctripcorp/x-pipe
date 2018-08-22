package com.ctrip.xpipe.redis.console.health;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.proxy.ProxyEnabled;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;

/**
 * @author chen.zhu
 * <p>
 * Aug 21, 2018
 */
public interface HealthCheckEndpoint extends Endpoint {

    int getHealthCheckTimeoutMilli();

    int getCommandTimeoutMilli();

    RedisMeta getRedisMeta();

    HostPort getHostPort();

    default boolean isProxyEnabled() {
        return this instanceof ProxyEnabled;
    }

}


