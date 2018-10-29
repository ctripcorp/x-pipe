package com.ctrip.xpipe.redis.proxy.monitor.session;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.lifecycle.AbstractStartStoppable;
import com.ctrip.xpipe.redis.core.monitor.BaseInstantaneousMetric;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author chen.zhu
 * <p>
 * Oct 29, 2018
 */
public class DefaultSessionStats extends AbstractStartStoppable implements SessionStats {

    private ScheduledFuture future;

    private ScheduledExecutorService scheduled;

    private AtomicLong inputBytes = new AtomicLong(0L);

    private AtomicLong outputBytes = new AtomicLong(0L);

    private BaseInstantaneousMetric inputMetric = new BaseInstantaneousMetric();

    private BaseInstantaneousMetric outputMetric = new BaseInstantaneousMetric();

    private volatile long lastUpdateTime = System.currentTimeMillis();

    public DefaultSessionStats(ScheduledExecutorService scheduled) {
        this.scheduled = scheduled;
    }

    @Override
    public void increaseInputBytes(long bytes) {
        updateLastTime();
        inputBytes.getAndAdd(bytes);
    }

    @Override
    public void increaseOutputBytes(long bytes) {
        updateLastTime();
        outputBytes.getAndAdd(bytes);
    }

    @Override
    public long lastUpdateTime() {
        return lastUpdateTime;
    }

    private void updateLastTime() {
        lastUpdateTime = System.currentTimeMillis();
    }

    @Override
    public long getInputBytes() {
        return inputBytes.get();
    }

    @Override
    public long getOutputBytes() {
        return outputBytes.get();
    }

    @Override
    public long getInputInstantaneousBPS() {
        return inputMetric.getInstantaneousMetric();
    }

    @Override
    public long getOutputInstantaneousBPS() {
        return outputMetric.getInstantaneousMetric();
    }

    private void updatePerSec() {
        int interval = 100;
        future = scheduled.scheduleAtFixedRate(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() {
                inputMetric.trackInstantaneousMetric(inputBytes.get());
                outputMetric.trackInstantaneousMetric(outputBytes.get());
            }
        }, interval, interval, TimeUnit.MILLISECONDS);
    }

    @Override
    protected void doStart() {
        updateLastTime();
        updatePerSec();
    }

    @Override
    protected void doStop() {
        if(future != null) {
            future.cancel(true);
        }
    }
}
