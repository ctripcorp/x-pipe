package com.ctrip.xpipe.redis.checker.healthcheck.actions.keeper.infoStats;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.redis.checker.healthcheck.KeeperHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.keeper.AbstractKeeperInfoCommand;
import com.ctrip.xpipe.redis.checker.healthcheck.session.Callbackable;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

public class KeeperInfoStatsAction extends AbstractKeeperInfoCommand<KeeperInfoStatsActionContext> {

    public KeeperInfoStatsAction(ScheduledExecutorService scheduled, KeeperHealthCheckInstance instance, ExecutorService executors) {
        super(scheduled, instance, executors);
    }

    @Override
    protected KeeperInfoStatsActionContext createActionContext(String extractor) {
        return new KeeperInfoStatsActionContext(instance, extractor);
    }

    @Override
    protected CommandFuture<String> executeRedisCommandForStats(Callbackable<String> callback) {
        return getActionInstance().getRedisSession().infoStats(callback);
    }
}
