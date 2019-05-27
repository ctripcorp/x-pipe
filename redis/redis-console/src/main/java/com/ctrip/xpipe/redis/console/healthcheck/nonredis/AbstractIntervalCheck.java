package com.ctrip.xpipe.redis.console.healthcheck.nonredis;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.alert.manager.AlertPolicyManager;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.spring.ConsoleContextConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public abstract class AbstractIntervalCheck {
    protected Logger logger = LoggerFactory.getLogger(getClass());

    @Resource(name = ConsoleContextConfig.SCHEDULED_EXECUTOR)
    protected ScheduledExecutorService scheduled;

    @Resource(name = ConsoleContextConfig.GLOBAL_EXECUTOR)
    protected Executor executors;

    @Autowired
    protected ConsoleConfig consoleConfig;

    @Autowired
    protected AlertPolicyManager alertPolicyManager;

    private long lastStartTime = System.currentTimeMillis();

    @PostConstruct
    public void postConstruct(){

        logger.info("[postConstruct]{}", this);

        for(ALERT_TYPE type : alertTypes()) {
            alertPolicyManager.markCheckInterval(type, this::getIntervalMilli);
        }

        scheduled.scheduleAtFixedRate(new AbstractExceptionLogTask() {

            @Override
            protected void doRun() throws Exception {
                checkStart();
            }
        },1, 30, TimeUnit.SECONDS);

    }

    protected void checkStart(){

        if(!shouldCheck()){
            return;
        }

        long current = System.currentTimeMillis();
        if( current - lastStartTime < getIntervalMilli()){
            logger.debug("[generatePlan][too quick {}, quit]", current - lastStartTime);
            return;
        }
        lastStartTime = current;

        executors.execute(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() throws Exception {
                doCheck();
            }
        });
    }

    protected abstract void doCheck();

    protected long getIntervalMilli(){
        return consoleConfig.getRedisConfCheckIntervalMilli();
    }

    protected abstract List<ALERT_TYPE> alertTypes();

    protected boolean shouldCheck() {
        return true;
    }
}
