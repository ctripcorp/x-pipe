package com.ctrip.xpipe.redis.checker.healthcheck.actions.keeper.infoStats;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.redis.checker.config.CheckerDbConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.KeeperHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.keeper.AbstractKeeperInfoCommand;
import com.ctrip.xpipe.redis.checker.healthcheck.session.Callbackable;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

public class KeeperInfoStatsAction extends AbstractKeeperInfoCommand<KeeperInfoStatsActionContext> {

    private CheckerDbConfig checkerDbConfig;

    public KeeperInfoStatsAction(ScheduledExecutorService scheduled, KeeperHealthCheckInstance instance, ExecutorService executors, CheckerDbConfig checkerDbConfig) {
        super(scheduled, instance, executors);
        this.checkerDbConfig = checkerDbConfig;
    }

    @Override
    protected KeeperInfoStatsActionContext createActionContext(String extractor) {
        return new KeeperInfoStatsActionContext(instance, extractor);
    }

    @Override
    protected CommandFuture<String> executeRedisCommandForStats(Callbackable<String> callback) {
        return getActionInstance().getRedisSession().info("", callback);
    }

    @Override
    protected int getBaseCheckInterval() {
        return getActionInstance().getHealthCheckConfig().getKeeperCheckerIntervalMilli();
    }

    @Override
    protected boolean shouldCheck(HealthCheckInstance instance){
        return super.shouldCheck(instance) && checkerDbConfig.isKeeperBalanceInfoCollectOn();
    }

}
