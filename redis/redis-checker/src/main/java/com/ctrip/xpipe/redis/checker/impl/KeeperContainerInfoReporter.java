package com.ctrip.xpipe.redis.checker.impl;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.checker.CheckerConsoleService;
import com.ctrip.xpipe.redis.checker.cluster.GroupCheckerLeaderAware;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.keeper.info.RedisUsedMemoryCollector;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.keeper.infoStats.KeeperFlowCollector;
import com.ctrip.xpipe.redis.checker.model.DcClusterShard;
import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel;
import com.ctrip.xpipe.tuple.Pair;
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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class KeeperContainerInfoReporter implements GroupCheckerLeaderAware {

    private RedisUsedMemoryCollector redisUsedMemoryCollector;

    private KeeperFlowCollector keeperFlowCollector;

    private ScheduledExecutorService scheduled;

    private DynamicDelayPeriodTask keeperContainerInfoReportTask;

    private CheckerConsoleService checkerConsoleService;

    private CheckerConfig config;

    private static final String CURRENT_IDC = FoundationService.DEFAULT.getDataCenter();

    private static final Logger logger = LoggerFactory.getLogger(HealthCheckReporter.class);


    public KeeperContainerInfoReporter(RedisUsedMemoryCollector redisUsedMemoryCollector, CheckerConsoleService
            checkerConsoleService, KeeperFlowCollector keeperFlowCollector, CheckerConfig config) {
        this.redisUsedMemoryCollector = redisUsedMemoryCollector;
        this.keeperFlowCollector = keeperFlowCollector;
        this.checkerConsoleService = checkerConsoleService;
        this.config = config;
    }

    @PostConstruct
    public void init() {
        logger.debug("[postConstruct] start");
        this.scheduled = Executors.newScheduledThreadPool(1, XpipeThreadFactory.create("KeeperContainerInfoReporter"));
        this.keeperContainerInfoReportTask = new DynamicDelayPeriodTask("KeeperContainerInfoReporter",
                this::reportKeeperContainerInfo, () -> 2 * config.getKeeperCheckerIntervalMilli(), scheduled);
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
    void reportKeeperContainerInfo() {
        try {
            logger.debug("[reportKeeperContainerInfo] start");
            Map<String, Map<DcClusterShard, Long>> hostPort2InputFlow = keeperFlowCollector.getHostPort2InputFlow();
            Map<DcClusterShard, Long> dcClusterShardUsedMemory = redisUsedMemoryCollector.getDcClusterShardUsedMemory();
            List<KeeperContainerUsedInfoModel> result = new ArrayList<>(hostPort2InputFlow.keySet().size());

            hostPort2InputFlow.forEach((keeperIp, inputFlowMap) -> {
                KeeperContainerUsedInfoModel model = new KeeperContainerUsedInfoModel();
                model.setKeeperIp(keeperIp).setDcName(CURRENT_IDC);
                long totalInputFlow = 0;
                long totalRedisUsedMemory = 0;
                Map<DcClusterShard, Pair<Long, Long>> detailInfo = new HashMap<>();
                for (Map.Entry<DcClusterShard, Long> entry : inputFlowMap.entrySet()) {
                    totalInputFlow += entry.getValue();
                    Long redisUsedMemory = dcClusterShardUsedMemory.get(entry.getKey());
                    if (redisUsedMemory == null) {
                        logger.warn("[reportKeeperContainerInfo] redisUsedMemory is null, dcClusterShard: {}", entry.getKey());
                        redisUsedMemory = 0L;
                    }
                    totalRedisUsedMemory += redisUsedMemory;

                    detailInfo.put(entry.getKey(), new Pair<>(entry.getValue(), redisUsedMemory));
                }

                model.setDetailInfo(detailInfo).setTotalInputFlow(totalInputFlow).setTotalRedisUsedMemory(totalRedisUsedMemory);
                result.add(model);
            });
            logger.debug("[reportKeeperContainerInfo] result: {}", result);
            checkerConsoleService.reportKeeperContainerInfo(config.getConsoleAddress(), result, config.getClustersPartIndex());
        } catch (Throwable th) {
            logger.error("[reportKeeperContainerInfo] fail", th);
        }
    }
}
