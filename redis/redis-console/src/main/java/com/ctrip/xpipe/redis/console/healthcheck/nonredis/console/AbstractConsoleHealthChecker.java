package com.ctrip.xpipe.redis.console.healthcheck.nonredis.console;

import com.ctrip.xpipe.api.cluster.CrossDcClusterServer;
import com.ctrip.xpipe.api.cluster.CrossDcLeaderAware;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.console.service.ConfigService;
import jakarta.annotation.PreDestroy;
import jakarta.annotation.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.SCHEDULED_EXECUTOR;

/**
 * @author chen.zhu
 * <p>
 * Nov 30, 2017
 */
public abstract class AbstractConsoleHealthChecker implements CrossDcLeaderAware {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired(required = false)
    private CrossDcClusterServer clusterServer;

    @Autowired
    protected ConfigService configService;

    @Autowired
    protected AlertManager alertManager;

    @Resource(name=SCHEDULED_EXECUTOR)
    private ScheduledExecutorService schedule;

    protected Future future;

    @Override
    public void isCrossDcLeader() {
        if(!stop()) {
            logger.info("[isCrossDcLeader] Cross DC Leader swapped, start alert in this console node");
            startFuture();
            logger.info("[isCrossDcLeader] Start alert in this console node successfully");
        }
    }

    @Override
    public void notCrossDcLeader() {
        logger.info("[notCrossDcLeader] Cross DC Leader failed, stop");
        cancelFuture();
        logger.info("[notCrossDcLeader] Stopped sending alert as no longer be a DC Leader");
    }

    private boolean shouldStart() {
        return clusterServer != null && clusterServer.amILeader();
    }

    public void startAlert() {
        if(!shouldStart()) {
            logger.info("[startAlert] not console leader, quit");
            return;
        }
        cancelFuture();
        startFuture();
        logger.info("[startAlert] Start future sending alert");
    }

    public void cancelFuture() {
        if(future != null && !(future.isCancelled() || future.isDone())) {
            future.cancel(true);
        }
    }

    public void startFuture() {
        logger.info("[startFuture] start sending alert of alert system down");
        int initDelay = 0, periodTime = 1;
        future = schedule.scheduleAtFixedRate(new OneHourPeriodTask(),
                initDelay, periodTime, TimeUnit.HOURS);
    }

    abstract boolean stop();

    abstract void alert();

    @PreDestroy
    public void whenShutDown() {
        if(future != null) {
            future.cancel(true);
        }
    }

    class OneHourPeriodTask implements Runnable {
        @Override
        public void run() {
            logger.info("[OneHourPeriodTask] do alert");
            try {
                if (stop()) {
                    logger.info("[OneHourPeriodTask] alert system is on, stop task");
                    future.cancel(true);
                    return;
                }
                alert();
            }catch (Exception e) {
                logger.error("[run] {}", e);
            }
        }
    }
}
