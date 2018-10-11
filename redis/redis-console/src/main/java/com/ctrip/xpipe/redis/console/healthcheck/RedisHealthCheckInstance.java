package com.ctrip.xpipe.redis.console.healthcheck;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.lifecycle.Lifecycle;
import com.ctrip.xpipe.redis.console.healthcheck.config.HealthCheckConfig;
import com.ctrip.xpipe.redis.console.healthcheck.session.RedisSession;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Aug 23, 2018
 */
public interface RedisHealthCheckInstance extends Lifecycle {

    RedisInstanceInfo getRedisInstanceInfo();

    HealthCheckConfig getHealthCheckConfig();

    Endpoint getEndpoint();

    RedisSession getRedisSession();

    void register(HealthCheckAction action);

    void unregister(HealthCheckAction action);

    List<HealthCheckAction> getHealthCheckActions();

}
