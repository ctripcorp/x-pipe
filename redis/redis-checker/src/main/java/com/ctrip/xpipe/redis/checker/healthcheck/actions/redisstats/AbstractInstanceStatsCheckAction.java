package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.redis.checker.healthcheck.*;
import com.ctrip.xpipe.redis.checker.healthcheck.leader.AbstractLeaderAwareHealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.session.Callbackable;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

public abstract class AbstractInstanceStatsCheckAction<T, K, V extends HealthCheckInstance> extends AbstractLeaderAwareHealthCheckAction<V> {


    protected CommandFuture<T> commandFuture;

    protected static final int METRIC_CHECK_INTERVAL = 60 * 1000;

    public AbstractInstanceStatsCheckAction(ScheduledExecutorService scheduled, V instance, ExecutorService executors) {
        super(scheduled, instance, executors);
    }

    @Override
    protected void doTask() {
        getHealthCheckLogger().debug("[doTask] begin for {}", instance);

        if (null != commandFuture && !commandFuture.isDone()) {
            getHealthCheckLogger().info("[doTask] last command {} hasn't finished!", commandFuture);
            return;
        }

        commandFuture = executeRedisCommandForStats(new Callbackable<T>() {
            @Override
            public void success(T message) {
                notifyListeners(generateActionContext(message));
            }

            @Override
            public void fail(Throwable throwable) {
                getHealthCheckLogger().info("[doTask] cmd execute fail for {}", instance, throwable);
            }
        });
    }

    @Override
    protected int getBaseCheckInterval() {
        return METRIC_CHECK_INTERVAL;
    }

    protected abstract CommandFuture<T> executeRedisCommandForStats(Callbackable<T> callback);

    protected abstract ActionContext<K, V> generateActionContext(T result);
}
