package com.ctrip.xpipe.redis.console.reporter;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.core.service.AbstractService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.GLOBAL_EXECUTOR;
import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.SCHEDULED_EXECUTOR;

public abstract class AbstractIntervalReport extends AbstractService {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    @Resource(name = SCHEDULED_EXECUTOR)
    protected ScheduledExecutorService scheduled;

    @Resource(name = GLOBAL_EXECUTOR)
    protected Executor executors;

    @Autowired
    protected ConsoleConfig consoleConfig;

    @PostConstruct
    public void postConstruct(){
        logger.info("[postConstruct]{}", this);

        scheduled.scheduleAtFixedRate(new AbstractExceptionLogTask() {

            @Override
            protected void doRun() throws Exception {
                reportStart();
            }
        }, 1, getIntervalMilli(), TimeUnit.MILLISECONDS);

    }

    protected void reportStart(){
        if(!shouldReport()){
            return;
        }
        executors.execute(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() throws Exception {
                doReport();
            }
        });
    }

    protected abstract void doReport();

    protected long getIntervalMilli(){
        return consoleConfig.getConsoleReportIntervalMill();
    }

    protected boolean shouldReport() {
        return true;
    }
}
