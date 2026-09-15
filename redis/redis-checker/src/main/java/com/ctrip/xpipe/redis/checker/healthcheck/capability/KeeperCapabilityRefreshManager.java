package com.ctrip.xpipe.redis.checker.healthcheck.capability;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.lifecycle.AbstractStartStoppable;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckInstanceManager;
import com.ctrip.xpipe.redis.checker.healthcheck.KeeperHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.KeeperInstanceInfo;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/** Independently refreshes capabilities for loaded TFS Keeper instances. */
@Component
public class KeeperCapabilityRefreshManager extends AbstractStartStoppable {

    private static final Logger logger = LoggerFactory.getLogger(KeeperCapabilityRefreshManager.class);
    private static final String THREAD_NAME = "keeper-capability-refresh";

    private final HealthCheckInstanceManager instanceManager;
    private final KeeperCapabilityCache capabilityCache;
    private final CheckerConfig checkerConfig;
    private final MetaCache metaCache;
    private final String currentDcId;
    private final ScheduledExecutorService scheduled;
    private final Object lifecycleLock = new Object();

    private volatile boolean running;
    private ScheduledFuture<?> future;

    @Autowired
    public KeeperCapabilityRefreshManager(HealthCheckInstanceManager instanceManager,
                                          KeeperCapabilityCache capabilityCache,
                                          CheckerConfig checkerConfig,
                                          MetaCache metaCache) {
        this(instanceManager, capabilityCache, checkerConfig, metaCache,
                FoundationService.DEFAULT.getDataCenter(), createScheduledExecutor());
    }

    @VisibleForTesting
    KeeperCapabilityRefreshManager(HealthCheckInstanceManager instanceManager,
                                   KeeperCapabilityCache capabilityCache,
                                   CheckerConfig checkerConfig,
                                   MetaCache metaCache,
                                   String currentDcId,
                                   ScheduledExecutorService scheduled) {
        this.instanceManager = Objects.requireNonNull(instanceManager, "instanceManager");
        this.capabilityCache = Objects.requireNonNull(capabilityCache, "capabilityCache");
        this.checkerConfig = Objects.requireNonNull(checkerConfig, "checkerConfig");
        this.metaCache = Objects.requireNonNull(metaCache, "metaCache");
        this.currentDcId = currentDcId;
        this.scheduled = Objects.requireNonNull(scheduled, "scheduled");
    }

    private static ScheduledExecutorService createScheduledExecutor() {
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1,
                XpipeThreadFactory.create(THREAD_NAME));
        executor.setRemoveOnCancelPolicy(true);
        return executor;
    }

    @Override
    public synchronized void start() throws Exception {
        try {
            super.start();
        } catch (Exception | Error throwable) {
            try {
                super.stop();
            } catch (Exception cleanupFailure) {
                throwable.addSuppressed(cleanupFailure);
            }
            throw throwable;
        }
    }

    @Override
    public synchronized void stop() throws Exception {
        super.stop();
    }

    @Override
    protected void doStart() {
        synchronized (lifecycleLock) {
            running = true;
            long interval = Math.max(1L, checkerConfig.getKeeperCapabilityRefreshIntervalMilli());
            try {
                future = scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {
                    @Override
                    protected void doRun() {
                        refreshOnce();
                    }
                }, 0, interval, TimeUnit.MILLISECONDS);
            } catch (RuntimeException | Error throwable) {
                running = false;
                throw throwable;
            }
        }
    }

    @Override
    protected void doStop() {
        synchronized (lifecycleLock) {
            running = false;
            try {
                if (future != null) {
                    future.cancel(true);
                }
            } finally {
                future = null;
                capabilityCache.invalidateAll();
            }
        }
    }

    @PreDestroy
    public void destroy() {
        try {
            if (isStarted()) {
                stop();
            }
        } catch (Exception e) {
            logger.warn("[destroy] failed to stop capability refresh", e);
        } finally {
            scheduled.shutdownNow();
        }
    }

    @VisibleForTesting
    void refreshOnce() {
        synchronized (lifecycleLock) {
            if (!running || !checkerConfig.isKeeperDelayCheckEnabled()) {
                return;
            }
            try {
                for (KeeperHealthCheckInstance instance : instanceManager.getAllKeeperInstance()) {
                    if (!running) {
                        return;
                    }
                    if (instance == null || !instance.isTfs() || !isInCurrentRegion(instance)) {
                        continue;
                    }
                    try {
                        capabilityCache.refresh(instance);
                    } catch (Throwable throwable) {
                        logger.warn("[refreshOnce] failed to refresh Keeper capability", throwable);
                    }
                }
            } catch (Throwable throwable) {
                logger.warn("[refreshOnce] failed to scan Keeper instances", throwable);
            }
        }
    }

    private boolean isInCurrentRegion(KeeperHealthCheckInstance instance) {
        KeeperInstanceInfo info = instance.getCheckInfo();
        String keeperDcId = info == null ? null : info.getDcId();
        if (StringUtil.isEmpty(currentDcId) || StringUtil.isEmpty(keeperDcId)) {
            logger.debug("[isInCurrentRegion] missing dc, currentDc={}, keeper={}", currentDcId, instance);
            return false;
        }
        if (currentDcId.equalsIgnoreCase(keeperDcId)) {
            return true;
        }
        try {
            if (metaCache.isCrossRegion(currentDcId, keeperDcId)) {
                logger.debug("[refreshOnce] skip cross-region Keeper, currentDc={}, keeperDc={}, keeper={}",
                        currentDcId, keeperDcId, info.getHostPort());
                return false;
            }
            return true;
        } catch (Throwable throwable) {
            logger.debug("[isInCurrentRegion] failed to resolve Keeper region, currentDc={}, keeperDc={}",
                    currentDcId, keeperDcId, throwable);
            return false;
        }
    }
}
