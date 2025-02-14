package com.ctrip.xpipe.redis.checker.impl;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.CheckerConsoleService;
import com.ctrip.xpipe.redis.checker.cluster.GroupCheckerLeaderAware;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisinfo.RedisMsgCollector;
import com.ctrip.xpipe.redis.checker.model.RedisMsg;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.ctrip.xpipe.utils.job.DynamicDelayPeriodTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class RedisMsgReporter implements GroupCheckerLeaderAware {

    private RedisMsgCollector redisMsgCollector;

    private ScheduledExecutorService scheduled;

    private DynamicDelayPeriodTask keeperContainerInfoReportTask;

    private CheckerConsoleService checkerConsoleService;

    private CheckerConfig config;


    private static final Logger logger = LoggerFactory.getLogger(HealthCheckReporter.class);


    public RedisMsgReporter(RedisMsgCollector redisMsgCollector, CheckerConsoleService
            checkerConsoleService, CheckerConfig config) {
        this.redisMsgCollector = redisMsgCollector;
        this.checkerConsoleService = checkerConsoleService;
        this.config = config;
    }

    @PostConstruct
    public void init() {
        logger.debug("[postConstruct] start");
        this.scheduled = Executors.newScheduledThreadPool(1, XpipeThreadFactory.create("KeeperContainerInfoReporter"));
        this.keeperContainerInfoReportTask = new DynamicDelayPeriodTask("KeeperContainerInfoReporter",
                this::reportKeeperContainerInfo, () -> config.getKeeperCheckerIntervalMilli(), scheduled);
    }

    @PreDestroy
    public void destroy() {
        try {
            keeperContainerInfoReportTask.stop();
            this.scheduled.shutdownNow();
        } catch (Throwable th) {
            logger.info("[preDestroy] fail", th);
        }
    }

    @Override
    public void isleader() {
        try {
            logger.debug("[isleader] become leader");
            keeperContainerInfoReportTask.start();
        } catch (Throwable th) {
            logger.info("[isleader] keeperContainerInfoReportTask start fail", th);
        }
    }

    @Override
    public void notLeader() {
        try {
            logger.debug("[notLeader] loss leader");
            keeperContainerInfoReportTask.stop();
        } catch (Throwable th) {
            logger.info("[notLeader] keeperContainerInfoReportTask stop fail", th);
        }
    }

    @VisibleForTesting
    public void reportKeeperContainerInfo() {
        try {
            Map<String, Map<HostPort, RedisMsg>> redisMsgMap = redisMsgCollector.getRedisMsgMap();
            checkerConsoleService.reportKeeperContainerInfo(config.getConsoleAddress(), redisMsgMap, config.getClustersPartIndex());
        } catch (Throwable th) {
            logger.error("[reportKeeperContainerInfo] fail", th);
        }
    }


}
