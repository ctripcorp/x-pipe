package com.ctrip.xpipe.redis.console.health;

import com.ctrip.xpipe.api.cluster.CrossDcClusterServer;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.spring.ConsoleContextConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author wenchao.meng
 *         <p>
 *         Aug 15, 2017
 */
public abstract class AbstractIntervalCheck {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    @Resource(name = ConsoleContextConfig.SCHEDULED_EXECUTOR)
    private ScheduledExecutorService scheduled;

    @Resource(name = ConsoleContextConfig.GLOBAL_EXECUTOR)
    protected Executor executors;

    @Autowired(required = false)
    private CrossDcClusterServer clusterServer;

    @Autowired
    protected ConsoleConfig consoleConfig;

    private long lastStartTime = System.currentTimeMillis();

    @PostConstruct
    public void postConstruct(){

        logger.info("[postConstruct]{}", this);

        scheduled.scheduleAtFixedRate(new AbstractExceptionLogTask() {

            @Override
            protected void doRun() throws Exception {
                checkStart();
            }
        },1, 1, TimeUnit.SECONDS);

    }

    protected void checkStart(){

        if(clusterServer != null && !clusterServer.amILeader()){
            logger.debug("[generatePlan][not leader quit]");
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


}
