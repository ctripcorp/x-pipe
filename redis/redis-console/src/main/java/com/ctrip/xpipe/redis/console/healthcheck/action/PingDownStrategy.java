package com.ctrip.xpipe.redis.console.healthcheck.action;

import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;

import java.util.Collection;
import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Sep 03, 2018
 */
public interface PingDownStrategy {

    PingDownResult getPingDownResult(Collection<RedisHealthCheckInstance> pingDownInstances);

    interface PingDownResult {

        List<RedisHealthCheckInstance> getPingDownInstances();

        List<RedisHealthCheckInstance> getIgnoredPingDownInstances();
    }
}
