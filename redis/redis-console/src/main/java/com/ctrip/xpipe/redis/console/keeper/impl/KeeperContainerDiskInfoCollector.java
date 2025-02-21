package com.ctrip.xpipe.redis.console.keeper.impl;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cache.TimeBoundCache;
import com.ctrip.xpipe.redis.checker.KeeperContainerCheckerService;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.console.cluster.ConsoleLeaderAware;
import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperDiskInfo;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.ctrip.xpipe.utils.job.DynamicDelayPeriodTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

@Component
public class KeeperContainerDiskInfoCollector implements ConsoleLeaderAware {

    @Autowired
    private CheckerConfig config;

    @Autowired
    private MetaCache metaCache;

    @Autowired
    private KeeperContainerCheckerService keeperContainerCheckerService;

    private ScheduledExecutorService scheduled;

    private DynamicDelayPeriodTask refreshTask;

    private static final String currentDc = FoundationService.DEFAULT.getDataCenter().toUpperCase();

    private TimeBoundCache<Map<String, KeeperDiskInfo>> keeperContainerDiskInfoCache;

    private static final Logger logger = LoggerFactory.getLogger(KeeperContainerDiskInfoCollector.class);

    @PostConstruct
    public void init() {
        this.scheduled = Executors.newScheduledThreadPool(1, XpipeThreadFactory.create("KeeperContainerDiskInfoCollector"));
        this.keeperContainerDiskInfoCache = new TimeBoundCache<>(config::getKeeperCheckerIntervalMilli, this::getCurrentDcAllKeeperContainerDiskInfo);
        this.refreshTask = new DynamicDelayPeriodTask("KeeperContainerDiskInfoCacheRefresh", keeperContainerDiskInfoCache::refresh,
                config::getKeeperCheckerIntervalMilli, scheduled);
    }

    private Map<String, KeeperDiskInfo> getCurrentDcAllKeeperContainerDiskInfo() {
        Map<String, KeeperDiskInfo> result = new HashMap<>();
        for (KeeperContainerMeta keeperContainer : metaCache.getXpipeMeta().findDc(currentDc).getKeeperContainers()) {
            try {
                result.put(keeperContainer.getIp(), keeperContainerCheckerService.getKeeperDiskInfo(keeperContainer.getIp()));
            } catch (Throwable th) {
                logger.error("[getCurrentDcAllKeeperContainerDiskInfo] getKeeperDiskInfo error, keeperIp: {}", keeperContainer.getIp(), th);
            }
        }
        return result;
    }

    public KeeperDiskInfo getKeeperDiskInfo(String ip) {
        Map<String, KeeperDiskInfo> data = keeperContainerDiskInfoCache.getData(false);
        if (data == null) {
            keeperContainerDiskInfoCache.refresh();
            data = keeperContainerDiskInfoCache.getData(false);
            if (data == null) {
                return null;
            }
        }
        return data.get(ip);
    }

    @Override
    public void isleader() {
        try {
            this.refreshTask.start();
        } catch (Throwable th) {
            logger.error("[doStart]", th);
        }
    }

    @Override
    public void notLeader() {
        try {
            this.refreshTask.stop();
        } catch (Throwable th) {
            logger.error("[doStop]", th);
        }
    }
}
