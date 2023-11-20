package com.ctrip.xpipe.redis.checker.healthcheck.actions.keeper;

import com.ctrip.xpipe.redis.checker.healthcheck.KeeperHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.AbstractInstanceStatsCheckAction;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

public abstract class KeeperStatsCheckAction<T, K> extends AbstractInstanceStatsCheckAction<T, K , KeeperHealthCheckInstance> {

    public KeeperStatsCheckAction(ScheduledExecutorService scheduled, KeeperHealthCheckInstance instance, ExecutorService executors) {
        super(scheduled, instance, executors);
    }
}
