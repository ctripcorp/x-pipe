package com.ctrip.xpipe.redis.console.healthcheck.redisconf;

import com.ctrip.xpipe.redis.console.alert.AlertManager;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author chen.zhu
 * <p>
 * Oct 06, 2018
 */
public abstract class RedisConfigCheckAction extends AbstractDCLAHealthCheckAction {

    private AtomicBoolean pass = new AtomicBoolean(false);

    protected AlertManager alertManager;

    private static final int START_TIME_INTERVAL_MILLI = 5 * 60 * 1000;

    public RedisConfigCheckAction(ScheduledExecutorService scheduled,
                                  RedisHealthCheckInstance instance,
                                  ExecutorService executors, AlertManager alertManager) {
        super(scheduled, instance, executors);
        this.alertManager = alertManager;
    }

    @Override
    protected int getCheckTimeInterval(int baseInterval) {
        return Math.abs(random.nextInt() % START_TIME_INTERVAL_MILLI);
    }

    @Override
    protected void doScheduledTask() {
        if(pass.get()) {
            scheduledFuture().cancel(true);
            getActionInstance().unregister(this);
        }
        doScheduledTask0();
    }

    protected abstract void doScheduledTask0();

    protected void checkPassed() {
        pass.set(true);
    }

    public boolean isCheckPassed() {
        return pass.get();
    }
}
