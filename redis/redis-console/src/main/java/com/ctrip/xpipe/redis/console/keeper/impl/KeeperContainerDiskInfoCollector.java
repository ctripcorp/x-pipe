package com.ctrip.xpipe.redis.console.keeper.impl;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.checker.KeeperContainerCheckerService;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.console.cluster.ConsoleLeaderAware;
import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperDiskInfo;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.ctrip.xpipe.utils.job.DynamicDelayPeriodTask;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

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

    private Map<String, KeeperDiskInfo> keeperContainerDiskInfoCache;

    private static final Logger logger = LoggerFactory.getLogger(KeeperContainerDiskInfoCollector.class);

    @PostConstruct
    public void init() {
        this.scheduled = Executors.newScheduledThreadPool(1, XpipeThreadFactory.create("KeeperContainerDiskInfoCollector"));
        this.keeperContainerDiskInfoCache = new HashMap<>();
        this.refreshTask = new DynamicDelayPeriodTask("KeeperContainerDiskInfoCacheRefresh", this::refresh,
                config::getKeeperCheckerIntervalMilli, scheduled);
    }

    private void refresh() {
        Map<String, KeeperDiskInfo> result = new HashMap<>();
        for (KeeperContainerMeta keeperContainer : metaCache.getXpipeMeta().findDc(currentDc).getKeeperContainers()) {
            try {
                result.put(keeperContainer.getIp(), keeperContainerCheckerService.getKeeperDiskInfo(keeperContainer.getIp()));
            } catch (Throwable th) {
                result.put(keeperContainer.getIp(), new KeeperDiskInfo());
                logger.error("[getCurrentDcAllKeeperContainerDiskInfo] getKeeperDiskInfo error, keeperIp: {}", keeperContainer.getIp(), th);
            }
        }
        keeperContainerDiskInfoCache = result;
    }

    public KeeperDiskInfo getKeeperDiskInfo(String ip) {
        return keeperContainerDiskInfoCache.get(ip);
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
