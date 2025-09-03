package com.ctrip.xpipe.redis.checker.healthcheck.actions.psubscribe;

import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HealthStatus;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.ping.PingActionListener;
import com.ctrip.xpipe.utils.VisibleForTesting;

import java.util.List;
import java.util.Map;

public interface PsubPingActionCollector {

    boolean supportInstance(RedisHealthCheckInstance instance);

    HealthStatus createHealthStatus(RedisHealthCheckInstance instance);

    PingActionListener createPingActionListener();

    PsubActionListener createPsubActionListener();

    @VisibleForTesting
    Map<RedisHealthCheckInstance, HealthStatus> getAllInstancesHealthStatus();

}
