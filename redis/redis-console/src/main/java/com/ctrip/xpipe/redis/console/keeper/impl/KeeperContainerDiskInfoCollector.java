package com.ctrip.xpipe.redis.console.keeper.impl;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.console.cluster.ConsoleLeaderAware;
import com.ctrip.xpipe.redis.console.service.KeeperContainerCheckerService;
import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperDiskInfo;
import com.ctrip.xpipe.redis.core.keeper.container.CurrentDcKeeperContainerAddressService;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.utils.StringUtil;
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

    @Autowired
    private CurrentDcKeeperContainerAddressService keeperContainerAddressService;

    private ScheduledExecutorService scheduled;

    private DynamicDelayPeriodTask refreshTask;

    private static final String currentDc = FoundationService.DEFAULT.getDataCenter().toUpperCase();

    /**
     * 按 keepercontainer_id 存，不按 IP：容器形态下 meta 里的 ip 是占位值（所有 pod 共用），
     * 按 IP 做 key 会让所有 pod 的磁盘信息互相覆盖。
     */
    private Map<Long, KeeperDiskInfo> keeperContainerDiskInfoCache;

    private static final Logger logger = LoggerFactory.getLogger(KeeperContainerDiskInfoCollector.class);

    @PostConstruct
    public void init() {
        this.scheduled = Executors.newScheduledThreadPool(1, XpipeThreadFactory.create("KeeperContainerDiskInfoCollector"));
        this.keeperContainerDiskInfoCache = new HashMap<>();
        this.refreshTask = new DynamicDelayPeriodTask("KeeperContainerDiskInfoCacheRefresh", this::refresh,
                config::getKeeperCheckerIntervalMilli, scheduled);
    }

    private void refresh() {
        Map<Long, KeeperDiskInfo> result = new HashMap<>();
        for (KeeperContainerMeta keeperContainer : metaCache.getXpipeMeta().findDc(currentDc).getKeeperContainers()) {
            Long keeperContainerId = keeperContainer.getId();
            if (keeperContainerId == null) {
                logger.warn("[refresh] keeperContainerId is null, ip: {}, hostname: {}", keeperContainer.getIp(), keeperContainer.getHostname());
                continue;
            }
            // 只处理本机房，所以用本机房的 ZK 缓存解析即可
            String ip = StringUtil.isEmpty(keeperContainer.getHostname())
                    ? keeperContainer.getIp()
                    : keeperContainerAddressService.getIpByHostname(keeperContainer.getHostname());
            if (StringUtil.isEmpty(ip)) {
                logger.warn("[refresh] cannot resolve ip, keeperContainerId: {}, hostname: {}", keeperContainerId, keeperContainer.getHostname());
                result.put(keeperContainerId, new KeeperDiskInfo());
                continue;
            }
            try {
                result.put(keeperContainerId, keeperContainerCheckerService.getKeeperDiskInfo(ip));
            } catch (Throwable th) {
                result.put(keeperContainerId, new KeeperDiskInfo());
                logger.error("[getCurrentDcAllKeeperContainerDiskInfo] getKeeperDiskInfo error, keeperContainerId: {}, keeperIp: {}", keeperContainerId, ip, th);
            }
        }
        keeperContainerDiskInfoCache = result;
    }

    public KeeperDiskInfo getKeeperDiskInfo(long keeperContainerId) {
        return keeperContainerDiskInfoCache.get(keeperContainerId);
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
