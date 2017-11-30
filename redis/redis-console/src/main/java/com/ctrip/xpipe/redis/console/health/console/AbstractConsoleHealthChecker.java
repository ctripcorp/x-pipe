package com.ctrip.xpipe.redis.console.health.console;

import com.ctrip.xpipe.api.cluster.CrossDcClusterServer;
import com.ctrip.xpipe.redis.console.alert.AlertManager;
import com.ctrip.xpipe.redis.console.service.ConfigService;
import com.ctrip.xpipe.redis.console.spring.ConsoleContextConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author chen.zhu
 * <p>
 * Nov 30, 2017
 */
public abstract class AbstractConsoleHealthChecker {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired(required = false)
    private CrossDcClusterServer clusterServer;

    @Autowired
    protected ConfigService configService;

    @Autowired
    protected AlertManager alertManager;

    @Resource(name = ConsoleContextConfig.SCHEDULED_EXECUTOR)
    protected ScheduledExecutorService schedule;

    protected Future future;

    protected boolean shouldStart() {
        return clusterServer != null && clusterServer.amILeader();
    }

    abstract boolean stop();

    abstract void alert();

    public void startAlert() {
        if(!shouldStart()) {
            logger.info("[startAlert] not console leader, quit");
            return;
        }
        int initDelay = 0, periodTime = 1;
        if(future != null && !(future.isCancelled() || future.isDone())) {
            future.cancel(true);
        }
        future = schedule.scheduleAtFixedRate(new OneHourPeriodTask(),
                initDelay, periodTime, TimeUnit.HOURS);
    }

    @PreDestroy
    public void whenShutDown() {
        if(future != null) {
            future.cancel(true);
        }
    }

    class OneHourPeriodTask implements Runnable {
        @Override
        public void run() {
            if(stop()) {
                future.cancel(true);
                return;
            }
            alert();
        }
    }
}
