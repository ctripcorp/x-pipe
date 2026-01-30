package com.ctrip.xpipe.redis.keeper.config;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@Component
public class ReplDelayConfigCache extends AbstractLifecycle implements TopElement {

    private KeeperCommonConfig keeperCommonConfig;

    private KeeperConfig keeperConfig;

    private ScheduledExecutorService scheduled;

    private ScheduledFuture<?> future;

    private Map<String, KeeperReplDelayConfig> keeperReplDelayConfigMap;

    private RedisReplDelayConfig redisReplDelayConfig;

    @Autowired
    public ReplDelayConfigCache(KeeperCommonConfig keeperCommonConfig, KeeperConfig keeperConfig) {
        this.keeperCommonConfig = keeperCommonConfig;
        this.keeperConfig = keeperConfig;
        this.redisReplDelayConfig = null;
        this.keeperReplDelayConfigMap = new HashMap<>();
    }

    private void refresh() {
        logger.debug("[refresh]");
        List<KeeperReplDelayConfig> keeperReplDelayConfigs = keeperCommonConfig.getKeeperReplDelayConfigs();
        Map<String, RedisReplDelayConfig> redisReplDelayConfigs = keeperCommonConfig.getRedisReplDelayConfigs();
        String currentDc = FoundationService.DEFAULT.getDataCenter();

        if (redisReplDelayConfigs.containsKey(currentDc)) {
            this.redisReplDelayConfig = redisReplDelayConfigs.get(currentDc);
        } else {
            this.redisReplDelayConfig = redisReplDelayConfigs.getOrDefault("DEFAULT", null);
        }

        Map<String, KeeperReplDelayConfig> localReplDelayConfigMap = new HashMap<>();
        for (KeeperReplDelayConfig config: keeperReplDelayConfigs) {
            if (currentDc.equalsIgnoreCase(config.getSrcDc())) {
                localReplDelayConfigMap.put(config.getDestDc().toUpperCase(), config);
            }
        }
        this.keeperReplDelayConfigMap = localReplDelayConfigMap;
    }

    @Nullable
    public KeeperReplDelayConfig getKeeperReplDelayConfig(String destIdc) {
        if (null == destIdc || this.keeperReplDelayConfigMap.isEmpty()) return null;
        return this.keeperReplDelayConfigMap.get(destIdc);
    }

    @Nullable
    public RedisReplDelayConfig getRedisReplDelayConfig() {
        return this.redisReplDelayConfig;
    }

    public int getCrossRegionBytesLimit() {
        return keeperCommonConfig.getCrossRegionBytesLimit();
    }

    public int getRedisMaxBytesLimit() {
        return keeperConfig.getRedisMaxBytesLimit();
    }

    public int getRedisMinBytesLimit() {
        return keeperConfig.getRedisMinBytesLimit();
    }

    public int getRedisRateCheckInterval() {
        return keeperConfig.getRedisRateCheckInterval();
    }

    @Override
    protected void doInitialize() throws Exception {
        super.doInitialize();
        this.scheduled = Executors.newScheduledThreadPool(1, XpipeThreadFactory.create("ReplDelayConfigCache"));
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        this.future = scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() throws Exception {
                ReplDelayConfigCache.this.refresh();
            }
        }, 5, 10, TimeUnit.SECONDS);
    }

    @Override
    protected void doStop() throws Exception {
        super.doStop();
        if (null != future) {
            this.future.cancel(false);
            this.future = null;
        }
    }

    @Override
    protected void doDispose() throws Exception {
        super.doDispose();
        if (null != scheduled) {
            this.scheduled.shutdownNow();
            this.scheduled = null;
        }
    }

}
