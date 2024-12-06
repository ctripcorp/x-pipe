package com.ctrip.xpipe.redis.keeper.config;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import org.springframework.beans.factory.annotation.Autowired;
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

    private KeeperCommonConfig keeperConfig;

    private ScheduledExecutorService scheduled;

    private ScheduledFuture<?> future;

    private Map<String, KeeperReplDelayConfig> keeperReplDelayConfigMap;

    public ReplDelayConfigCache() {
        this(null);
    }

    @Autowired
    public ReplDelayConfigCache(KeeperCommonConfig keeperConfig) {
        this.keeperConfig = keeperConfig;
        this.keeperReplDelayConfigMap = new HashMap<>();
    }

    private void refresh() {
        logger.debug("[refresh]");
        List<KeeperReplDelayConfig> replDelayConfigs = keeperConfig.getReplDelayConfigs();
        Map<String, KeeperReplDelayConfig> localReplDelayConfigMap = new HashMap<>();
        String currentDc = FoundationService.DEFAULT.getDataCenter();
        for (KeeperReplDelayConfig config: replDelayConfigs) {
            if (currentDc.equalsIgnoreCase(config.getSrcDc())) {
                localReplDelayConfigMap.put(config.getDestDc().toUpperCase(), config);
            }
        }
        this.keeperReplDelayConfigMap = localReplDelayConfigMap;
    }

    public KeeperReplDelayConfig getReplDelayConfig(String destIdc) {
        if (null == destIdc || this.keeperReplDelayConfigMap.isEmpty()) return null;
        return this.keeperReplDelayConfigMap.get(destIdc);
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
