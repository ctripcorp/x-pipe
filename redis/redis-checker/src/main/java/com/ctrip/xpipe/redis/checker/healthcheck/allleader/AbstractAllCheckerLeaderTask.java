package com.ctrip.xpipe.redis.checker.healthcheck.allleader;

import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.manager.AlertPolicyManager;
import com.ctrip.xpipe.redis.checker.cluster.AllCheckerLeaderAware;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.Resource;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.SCHEDULED_EXECUTOR;

public abstract class AbstractAllCheckerLeaderTask extends AbstractLifecycle implements AllCheckerLeaderAware {
    public abstract void doTask();
    
    public abstract int getDelay();
    
    public abstract boolean shouldCheck();
    
    private ScheduledFuture<?> future;
    
    @Resource(name = SCHEDULED_EXECUTOR)
    private ScheduledExecutorService schedule;
    @Autowired
    AlertPolicyManager alertPolicyManager;

    @Override
    public void isleader() {
         for(ALERT_TYPE type: alertTypes()) {
             alertPolicyManager.markCheckInterval(type, this::getDelay);
         }
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

    protected  abstract List<ALERT_TYPE> alertTypes();
}
