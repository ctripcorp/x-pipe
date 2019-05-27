package com.ctrip.xpipe.redis.proxy.monitor.stats;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.lifecycle.AbstractStartStoppable;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author chen.zhu
 * <p>
 * Oct 31, 2018
 */
public abstract class AbstractStats extends AbstractStartStoppable {

    private ScheduledFuture future;

    private ScheduledExecutorService scheduled;

    private static final int ONE_SEC = 1000;

    public AbstractStats(ScheduledExecutorService scheduled) {
        this.scheduled = scheduled;
    }

    @Override
    protected void doStart() {
        future = scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() {
                doTask();
            }
        }, getCheckIntervalMilli(), getCheckIntervalMilli(), TimeUnit.MILLISECONDS);
    }

    @Override
    protected void doStop() {
        if(future != null) {
            future.cancel(true);
        }
    }

    protected ScheduledFuture getFuture() {
        return future;
    }

    protected ScheduledExecutorService getScheduled() {
        return scheduled;
    }

    protected int getCheckIntervalMilli() {
        return ONE_SEC;
    }

    protected abstract void doTask();
}
