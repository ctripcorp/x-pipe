package com.ctrip.xpipe.redis.console.healthcheck;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.lifecycle.Lifecycle;
import com.ctrip.xpipe.redis.console.health.RedisSession;
import com.ctrip.xpipe.redis.console.healthcheck.config.HealthCheckConfig;

/**
 * @author chen.zhu
 * <p>
 * Aug 23, 2018
 */
public interface RedisHealthCheckInstance extends Lifecycle {

    HealthCheckContext getHealthCheckContext();

    RedisInstanceInfo getRedisInstanceInfo();

    HealthCheckConfig getHealthCheckConfig();

    Endpoint getEndpoint();

    RedisSession getRedisSession();

    HealthStatusManager getHealthStatusManager();

    default void markDown(HealthStatusManager.MarkDownReason markDownReason) {
        getHealthStatusManager().markDown(this, markDownReason);
    }

    default void markUp(HealthStatusManager.MarkUpReason markUpReason) {
        getHealthStatusManager().markUp(this, markUpReason);
    }

}
