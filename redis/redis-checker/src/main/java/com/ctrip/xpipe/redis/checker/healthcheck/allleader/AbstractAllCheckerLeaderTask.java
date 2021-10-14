package com.ctrip.xpipe.redis.checker.healthcheck.allleader;

import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.redis.checker.cluster.AllCheckerLeaderAware;

import javax.annotation.Resource;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.SCHEDULED_EXECUTOR;

public abstract class AbstractAllCheckerLeaderTask extends AbstractLifecycle implements AllCheckerLeaderAware {
    public abstract void doTask();
    
    public abstract Long getDelay();
    
    public abstract boolean shouldCheck();
    
    private ScheduledFuture<?> future;
    
    @Resource(name = SCHEDULED_EXECUTOR)
    private ScheduledExecutorService schedule;

    @Override
    public void isleader() {
        future = schedule.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    if (shouldCheck()) {
                        doTask();
                    }
                } catch (Throwable th) {
                    logger.error("[run error] {}", th);
                }
            }
        }, 0, getDelay(), TimeUnit.MILLISECONDS);
    }
    
    @Override
    public void notLeader() {
        if(future != null) {
            future.cancel(true);
            future = null;
        }
    }
}
