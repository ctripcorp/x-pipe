package com.ctrip.xpipe.redis.checker.healthcheck.actions.inforeplid;

import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HealthStatus;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.ping.PingActionListener;
import com.ctrip.xpipe.utils.VisibleForTesting;

import java.util.Map;

public interface InfoReplIdPingActionCollector {

    boolean supportInstance(RedisHealthCheckInstance instance);

    HealthStatus createHealthStatus(RedisHealthCheckInstance instance);

    PingActionListener createPingActionListener();

    InfoReplIdActionListener createInfoReplIdActionListener();

    @VisibleForTesting
    Map<RedisHealthCheckInstance, HealthStatus> getAllInstancesHealthStatus();

}
