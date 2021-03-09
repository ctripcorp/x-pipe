package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.tombstonesize;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.RedisStatsCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.session.Callbackable;
import org.slf4j.Logger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

public class TombstoneSizeCheckAction extends RedisStatsCheckAction<Long, Long> {

    public TombstoneSizeCheckAction(ScheduledExecutorService scheduled, RedisHealthCheckInstance instance, ExecutorService executors) {
        super(scheduled, instance, executors);
    }

    @Override
    protected Logger getHealthCheckLogger() {
        return logger;
    }

    @Override
    protected CommandFuture<Long> executeRedisCommandForStats(Callbackable<Long> callback) {
        return instance.getRedisSession().tombstoneSize(callback);
    }

    @Override
    protected TombstoneSizeActionContext generateActionContext(Long result) {
        return new TombstoneSizeActionContext(instance, result);
    }

}
