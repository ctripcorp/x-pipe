package com.ctrip.xpipe.redis.checker.impl;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.checker.CheckerConsoleService;
import com.ctrip.xpipe.redis.checker.KeeperContainerCheckerService;
import com.ctrip.xpipe.redis.checker.cluster.GroupCheckerLeaderAware;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.keeper.info.RedisUsedMemoryCollector;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.keeper.infoStats.KeeperFlowCollector;
import com.ctrip.xpipe.redis.checker.model.DcClusterShard;
import com.ctrip.xpipe.redis.checker.model.DcClusterShardKeeper;
import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel;
import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel.*;
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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class KeeperContainerInfoReporter implements GroupCheckerLeaderAware {

    private RedisUsedMemoryCollector redisUsedMemoryCollector;

    private KeeperFlowCollector keeperFlowCollector;

    private ScheduledExecutorService scheduled;

    private DynamicDelayPeriodTask keeperContainerInfoReportTask;

    private CheckerConsoleService checkerConsoleService;

    private KeeperContainerCheckerService keeperContainerService;

    private CheckerConfig config;

    private MetaCache metaCache;

    private static final String CURRENT_IDC = FoundationService.DEFAULT.getDataCenter();

    private static final Logger logger = LoggerFactory.getLogger(HealthCheckReporter.class);


    public KeeperContainerInfoReporter(RedisUsedMemoryCollector redisUsedMemoryCollector, CheckerConsoleService
            checkerConsoleService, KeeperFlowCollector keeperFlowCollector, CheckerConfig config, KeeperContainerCheckerService keeperContainerService, MetaCache metaCache) {
        this.redisUsedMemoryCollector = redisUsedMemoryCollector;
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
            logger.debug("[reportKeeperContainerInfo] start");
            Map<String, Map<DcClusterShardKeeper, Long>> collectedInfos = keeperFlowCollector.getHostPort2InputFlow();
            Map<String, Map<DcClusterShardKeeper, Long>> hostPort2InputFlow = new HashMap<>();
            for (DcMeta dcMeta : metaCache.getXpipeMeta().getDcs().values()) {
                if (CURRENT_IDC.equalsIgnoreCase(dcMeta.getId())) {
                    dcMeta.getKeeperContainers().forEach(keeperContainerMeta -> {
                        if (collectedInfos.containsKey(keeperContainerMeta.getIp())) {
                            hostPort2InputFlow.put(keeperContainerMeta.getIp(), collectedInfos.get(keeperContainerMeta.getIp()));
                        } else {
                            hostPort2InputFlow.put(keeperContainerMeta.getIp(), new ConcurrentHashMap<>());
                        }
                    });
                }
            }
            Map<DcClusterShard, Long> dcClusterShardUsedMemory = redisUsedMemoryCollector.getDcClusterShardUsedMemory();
            List<KeeperContainerUsedInfoModel> result = new ArrayList<>(hostPort2InputFlow.keySet().size());
            hostPort2InputFlow.forEach((keeperIp, inputFlowMap) -> {
                KeeperContainerUsedInfoModel model = new KeeperContainerUsedInfoModel();
                model.setKeeperIp(keeperIp).setDcName(CURRENT_IDC);
                long activeInputFlow = 0;
                long totalInputFlow = 0;
                long activeRedisUsedMemory = 0;
                long totalRedisUsedMemory = 0;
                int activeKeeperCount = 0;
                int totalKeeperCount = 0;
                Map<DcClusterShardKeeper, KeeperUsedInfo> detailInfo = new HashMap<>();
                for (Map.Entry<DcClusterShardKeeper, Long> entry : inputFlowMap.entrySet()) {
                    totalKeeperCount++;
                    totalInputFlow += entry.getValue();
                    DcClusterShardKeeper dcClusterShardKeeper = entry.getKey();
                    long inputFlow = entry.getValue();
                    Long redisUsedMemory = dcClusterShardUsedMemory.get(new DcClusterShard().setDcId(dcClusterShardKeeper.getDcId()).setClusterId(dcClusterShardKeeper.getClusterId()).setShardId(dcClusterShardKeeper.getShardId()));
                    if (redisUsedMemory == null) {
                        logger.warn("[reportKeeperContainerInfo] redisUsedMemory is null, dcClusterShard: {}", entry.getKey());
                        redisUsedMemory = 0L;
                    }
                    totalRedisUsedMemory += redisUsedMemory;
                    if (dcClusterShardKeeper.isActive()) {
                        activeRedisUsedMemory += redisUsedMemory;
                        activeInputFlow += inputFlow;
                        activeKeeperCount++;
                    }
                    detailInfo.put(dcClusterShardKeeper, new KeeperUsedInfo(redisUsedMemory, inputFlow, keeperIp));

                }
                try {
                    logger.debug("[get KeeperContainer disk Info] keeperIp: {}", keeperIp);
                    KeeperDiskInfo keeperDiskInfo = keeperContainerService.getKeeperDiskInfo(keeperIp);
                    logger.debug("[KeeperContainer disk Info] keeperIp: {} keeperDiskInfo: {}", keeperIp, keeperDiskInfo);
                    if  (keeperDiskInfo != null && keeperDiskInfo.available && keeperDiskInfo.spaceUsageInfo != null) {
                        model.setDiskAvailable(true)
                                .setDiskSize(keeperDiskInfo.spaceUsageInfo.size)
                                .setDiskUsed(keeperDiskInfo.spaceUsageInfo.use);
                    } else {
                        model.setDiskAvailable(false)
                                .setKeeperContainerHealth(false);
                    }
                } catch (Throwable e){
                    logger.error("[reportKeeperContainerInfo] getKeeperDiskInfo error, keeperIp: {}", keeperIp, e);
                }
                model.setDetailInfo(detailInfo)
                        .setActiveKeeperCount(activeKeeperCount)
                        .setTotalKeeperCount(totalKeeperCount)
                        .setActiveInputFlow(activeInputFlow)
                        .setActiveRedisUsedMemory(activeRedisUsedMemory)
                        .setTotalInputFlow(totalInputFlow)
                        .setTotalRedisUsedMemory(totalRedisUsedMemory);
                result.add(model);
            });
            logger.debug("[reportKeeperContainerInfo] result: {}", result);
            checkerConsoleService.reportKeeperContainerInfo(config.getConsoleAddress(), result, config.getClustersPartIndex());
        } catch (Throwable th) {
            logger.error("[reportKeeperContainerInfo] fail", th);
        }
    }


}
