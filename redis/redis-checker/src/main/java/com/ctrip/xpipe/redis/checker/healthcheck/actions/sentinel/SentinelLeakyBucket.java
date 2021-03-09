package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel;

import com.ctrip.xpipe.lifecycle.AbstractStartStoppable;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.utils.DefaultLeakyBucket;
import com.ctrip.xpipe.utils.LeakyBucket;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author chen.zhu
 * <p>
 * May 19, 2020
 */
public class SentinelLeakyBucket extends AbstractStartStoppable implements LeakyBucket {

    private CheckerConfig checkerConfig;

    private LeakyBucket origin;

    private ScheduledExecutorService scheduled;

    private ScheduledFuture<?> future;

    public static int PERIODIC_MILLI = 1000;

    public SentinelLeakyBucket(CheckerConfig config, ScheduledExecutorService scheduled) {
        this.checkerConfig = config;
        this.scheduled = scheduled;
        this.origin = new DefaultLeakyBucket(checkerConfig.getSentinelRateLimitSize());
    }

    @Override
    public boolean tryAcquire() {
        if(isOpen()) {
            return origin.tryAcquire();
        }
        return true;
    }

    @Override
    public void release() {
        if(isOpen()) {
            origin.release();
        }
    }

    public void delayRelease(long time, TimeUnit timeUnit) {
        scheduled.schedule(new Runnable() {
            @Override
            public void run() {
                release();
            }
        }, time, timeUnit);
    }

    @Override
    public void resize(int newSize) {
        if(isOpen()) {
            origin.resize(newSize);
        }
    }

    @Override
    public int references() {
        return origin.references();
    }

    @Override
    public int getTotalSize() {
        return origin.getTotalSize();
    }

    private boolean isOpen() {
        return checkerConfig.isSentinelRateLimitOpen();
    }

    @Override
    protected void doStart() throws Exception {
        future = scheduled.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                if(!isOpen()) {
                    return;
                }
                if(getTotalSize() != checkerConfig.getSentinelRateLimitSize()) {
                    resize(checkerConfig.getSentinelRateLimitSize());
                }
            }
        }, PERIODIC_MILLI, PERIODIC_MILLI, TimeUnit.MILLISECONDS);
    }

    @Override
    protected void doStop() throws Exception {
        if(future != null) {
            future.cancel(true);
            future = null;
        }
    }
}
