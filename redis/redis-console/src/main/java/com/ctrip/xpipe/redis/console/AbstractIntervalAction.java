package com.ctrip.xpipe.redis.console;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.manager.AlertPolicyManager;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.GLOBAL_EXECUTOR;
import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.SCHEDULED_EXECUTOR;

public abstract class AbstractIntervalAction {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    @Resource(name = SCHEDULED_EXECUTOR)
    protected ScheduledExecutorService scheduled;

    @Resource(name = GLOBAL_EXECUTOR)
    protected Executor executors;

    @Autowired
    protected AlertPolicyManager alertPolicyManager;

    @Autowired
    protected ConsoleConfig consoleConfig;

    private long lastStartTime = System.currentTimeMillis();

    @PostConstruct
    public void postConstruct(){
        logger.info("[postConstruct] {}", this);

        for(ALERT_TYPE type : alertTypes()) {
            alertPolicyManager.markCheckInterval(type, this::getIntervalMilli);
        }

        scheduled.scheduleAtFixedRate(() -> {
            actionStart();
        }, 1000, getLeastIntervalMilli(), TimeUnit.MILLISECONDS);
    }

    protected void actionStart(){
        if(!shouldDoAction()){
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
                doAction();
            }
        });
    }

    protected abstract void doAction();

    protected long getIntervalMilli() {
        return consoleConfig.getRedisConfCheckIntervalMilli();
    }

    protected abstract List<ALERT_TYPE> alertTypes();

    protected long getLeastIntervalMilli() {
        return 30000L;
    }

    protected boolean shouldDoAction() {
        return true;
    }
}
