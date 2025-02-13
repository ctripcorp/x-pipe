package com.ctrip.xpipe.redis.checker.impl;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.CheckerConsoleService;
import com.ctrip.xpipe.redis.checker.KeeperContainerCheckerService;
import com.ctrip.xpipe.redis.checker.cluster.GroupCheckerLeaderAware;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.keeper.info.RedisMsgCollector;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.keeper.infoStats.KeeperFlowCollector;
import com.ctrip.xpipe.redis.checker.model.DcClusterShard;
import com.ctrip.xpipe.redis.checker.model.DcClusterShardKeeper;
import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel;
import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel.*;
import com.ctrip.xpipe.redis.checker.model.RedisMsg;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperDiskInfo;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.ctrip.xpipe.utils.job.DynamicDelayPeriodTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class RedisMsgReporter implements GroupCheckerLeaderAware {

    private RedisMsgCollector redisMsgCollector;

    private KeeperFlowCollector keeperFlowCollector;

    private ScheduledExecutorService scheduled;

    private DynamicDelayPeriodTask keeperContainerInfoReportTask;

    private CheckerConsoleService checkerConsoleService;

    private KeeperContainerCheckerService keeperContainerService;

    private CheckerConfig config;

    private MetaCache metaCache;

    private static final String CURRENT_IDC = FoundationService.DEFAULT.getDataCenter();

    private static final Logger logger = LoggerFactory.getLogger(HealthCheckReporter.class);


    public RedisMsgReporter(RedisMsgCollector redisMsgCollector, CheckerConsoleService
            checkerConsoleService, KeeperFlowCollector keeperFlowCollector, CheckerConfig config, KeeperContainerCheckerService keeperContainerService, MetaCache metaCache) {
        this.redisMsgCollector = redisMsgCollector;
        this.keeperFlowCollector = keeperFlowCollector;
        this.checkerConsoleService = checkerConsoleService;
        this.config = config;
        this.keeperContainerService = keeperContainerService;
        this.metaCache = metaCache;
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
