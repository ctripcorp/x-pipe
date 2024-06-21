package com.ctrip.xpipe.redis.checker.healthcheck.actions.keeper.info;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.redis.checker.config.CheckerDbConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.AbstractInfoCommandAction;
import com.ctrip.xpipe.redis.checker.healthcheck.session.Callbackable;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

public class RedisInfoAction extends AbstractInfoCommandAction<RedisInfoActionContext> {

    private CheckerDbConfig checkerDbConfig;

    public RedisInfoAction(ScheduledExecutorService scheduled, RedisHealthCheckInstance instance, ExecutorService executors, CheckerDbConfig checkerDbConfig) {
        super(scheduled, instance, executors);
        this.checkerDbConfig = checkerDbConfig;
    }

    @Override
    protected RedisInfoActionContext createActionContext(String extractor) {
        return new RedisInfoActionContext(instance, extractor);
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
