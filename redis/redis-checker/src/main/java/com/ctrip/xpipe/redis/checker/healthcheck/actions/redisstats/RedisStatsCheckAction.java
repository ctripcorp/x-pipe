package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats;

import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

public abstract class RedisStatsCheckAction<T, K> extends AbstractInstanceStatsCheckAction<T, K ,RedisHealthCheckInstance> {

    public RedisStatsCheckAction(ScheduledExecutorService scheduled, RedisHealthCheckInstance instance, ExecutorService executors) {
        super(scheduled, instance, executors);
    }
}
