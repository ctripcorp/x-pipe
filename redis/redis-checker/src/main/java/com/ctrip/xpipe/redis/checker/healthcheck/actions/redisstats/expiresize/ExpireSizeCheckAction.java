package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.expiresize;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.RedisStatsCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.session.Callbackable;
import org.slf4j.Logger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

public class ExpireSizeCheckAction extends RedisStatsCheckAction<Long, Long> {

    public ExpireSizeCheckAction(ScheduledExecutorService scheduled, RedisHealthCheckInstance instance, ExecutorService executors) {
        super(scheduled, instance, executors);
    }

    @Override
    protected Logger getHealthCheckLogger() {
        return logger;
    }

    @Override
    protected CommandFuture<Long> executeRedisCommandForStats(Callbackable<Long> callback) {
        return instance.getRedisSession().expireSize(callback);
    }

    @Override
    protected ExpireSizeActionContext generateActionContext(Long result) {
        return new ExpireSizeActionContext(instance, result);
    }

}
