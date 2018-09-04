package com.ctrip.xpipe.redis.console.healthcheck;

import com.ctrip.xpipe.api.lifecycle.Startable;
import com.ctrip.xpipe.api.lifecycle.Stoppable;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;

import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author chen.zhu
 * <p>
 * Aug 28, 2018
 */
public abstract class BaseContext extends AbstractLifecycle {

    protected ScheduledExecutorService scheduled;

    protected RedisHealthCheckInstance instance;

    private ScheduledFuture future;

    private static Random random = new Random();

    private static int DELTA = 100;

    public BaseContext(ScheduledExecutorService scheduled, RedisHealthCheckInstance instance) {
        this.scheduled = scheduled;
        this.instance = instance;
    }

    protected void scheduleTask(int baseInterval) {
        long checkInterval = getCheckTimeInterval(baseInterval);
        future = scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {

            @Override
            protected void doRun() {
                doScheduledTask();
            }
        }, checkInterval, checkInterval, TimeUnit.MILLISECONDS);
    }

    private int getCheckTimeInterval(int baseInterval) {
        return baseInterval + (((Math.abs(random.nextInt()) * DELTA) % baseInterval) >> 1);
    }

    protected int getWarmupTime() {
        int base = instance
                .getHealthCheckConfig()
                .checkIntervalMilli();
        int result = (Math.abs(random.nextInt()) % base);
        if(result == 0) {
            result += DELTA;
        }
        if(result == getBaseCheckInterval()) {
            result -= DELTA;
        }
        return result;
    }

    protected abstract void doScheduledTask();

    protected int getBaseCheckInterval() {
        return instance.getHealthCheckConfig().checkIntervalMilli();
    }

    @Override
    public void doStart() throws Exception {
        super.doStart();
        scheduleTask(getBaseCheckInterval());
    }

    @Override
    public void doStop() throws Exception {
        if(future != null) {
            future.cancel(true);
        }
        super.doStop();
    }
}
