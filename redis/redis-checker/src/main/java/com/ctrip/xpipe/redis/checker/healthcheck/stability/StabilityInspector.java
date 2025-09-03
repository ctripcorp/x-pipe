package com.ctrip.xpipe.redis.checker.healthcheck.stability;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.redis.checker.CheckerConsoleService;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.DefaultDelayPingActionCollector;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HEALTH_STATE;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.ctrip.xpipe.utils.job.DynamicDelayPeriodTask;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @author lishanglin
 * date 2022/8/5
 */
@Component
public class StabilityInspector extends AbstractLifecycle implements TopElement {

    private DefaultDelayPingActionCollector collector;

    private MetaCache metaCache;

    private CheckerConfig config;

    private CheckerConsoleService checkerConsoleService;

    private AtomicBoolean siteStable = new AtomicBoolean(true);
    private AtomicBoolean dcIsolated = new AtomicBoolean(false);

    private AtomicInteger continuousMismatchTimes = new AtomicInteger();

    private AtomicInteger continuousNoInterested = new AtomicInteger();

    private AtomicInteger isolatedContinuousMismatchTimes = new AtomicInteger();

    private DynamicDelayPeriodTask task;

    private ScheduledExecutorService scheduled;

    private static final String TYPE = "stability";
    private static final String DC_ISOLATED_TYPE = "isolated";

    public StabilityInspector() {
    }

    public StabilityInspector(DefaultDelayPingActionCollector defaultDelayPingActionCollector, MetaCache metaCache,
                              CheckerConfig checkerConfig, CheckerConsoleService checkerConsoleService) {
        this.collector = defaultDelayPingActionCollector;
        this.metaCache = metaCache;
        this.config = checkerConfig;
        this.checkerConsoleService = checkerConsoleService;
    }

    protected boolean isSiteStable() {
        return siteStable.get() && !dcIsolated.get();
    }

    @VisibleForTesting
    protected void inspect() {
        logger.debug("[inspect] begin");
        checkSiteStable();
        checkDcIsolated();
    }

    void checkSiteStable() {
        try {
            Map<HostPort, HEALTH_STATE> currentAllHealthStates = collector.getAllCachedState();
            String currentDc = FoundationService.DEFAULT.getDataCenter();
            Set<HostPort> interested = currentAllHealthStates.keySet().stream()
                    .filter(hostPort -> {
                        try {
                            return currentDc.equalsIgnoreCase(metaCache.getDc(hostPort))
                                    && currentDc.equalsIgnoreCase(metaCache.getActiveDc(hostPort));
                        } catch (Throwable th) {
                            logger.debug("[inspect][ignore instance] {}", hostPort, th);
                            return false;
                        }
                    }).collect(Collectors.toSet());

            if (interested.isEmpty()) {
                if (continuousNoInterested.incrementAndGet() < 0) continuousNoInterested.set(0);
            } else {
                continuousNoInterested.set(0);

                int upCnt = 0;
                int downCnt = 0;
                for (HostPort hostPort : interested) {
                    HEALTH_STATE healthState = currentAllHealthStates.get(hostPort);
                    if (HEALTH_STATE.HEALTHY.equals(healthState)) upCnt++;
                    if (HEALTH_STATE.DOWN.equals(healthState)) downCnt++;
                }

                boolean mayStable = siteStable.get();
                if ((upCnt * 1.0 / interested.size()) > config.getSiteStableThreshold()) {
                    mayStable = true;
                } else if ((downCnt * 1.0 / interested.size()) > config.getSiteUnstableThreshold()) {
                    mayStable = false;
                }

                incrMismatchIfNeeded(mayStable);
            }

            toggleStableIfNeeded();
            EventMonitor.DEFAULT.logEvent(TYPE, siteStable.get() ? "stable" : "unstable");
        } catch (Throwable th) {
            logger.error("[checkSiteStable]", th);
        }
    }

    private void incrMismatchIfNeeded(boolean mayStable) {
        if (mayStable != siteStable.get()) {
            int after = continuousMismatchTimes.incrementAndGet();
            logger.info("[incrMismatchIfNeeded] may {}:{}", mayStable ? "stable":"unstable", after);
        } else if (continuousMismatchTimes.get() > 0) {
            logger.info("[incrMismatchIfNeeded][reset]");
            this.continuousMismatchTimes.set(0);
        }
    }

    private void toggleStableIfNeeded() {
        if (siteStable.get() && continuousMismatchTimes.get() >= config.getStableLossAfterRounds()) {
            logger.info("[toggleStableIfNeeded] become unstable");
            setSiteStable(false);
        } else if (!siteStable.get() && continuousMismatchTimes.get() >= config.getStableRecoverAfterRounds()) {
            logger.info("[toggleStableIfNeeded] become stable");
            setSiteStable(true);
        } else if (!siteStable.get() && continuousNoInterested.get() >= config.getStableResetAfterRounds()) {
            logger.info("[toggleStableIfNeeded][continue no interested] become stable");
            setSiteStable(true);
        }
    }

    private void checkDcIsolated() {
        try {
            boolean mayIsolated = checkerConsoleService.dcIsolated(config.getConsoleAddress());
            incrIsolatedMismatchIfNeeded(mayIsolated);
            toggleDcIsolatedIfNeeded();
            EventMonitor.DEFAULT.logEvent(DC_ISOLATED_TYPE, dcIsolated.get() ? "isolated" : "unisolated");
        } catch (Throwable th) {
            logger.error("[dcIsolated]get from console:{} failed", config.getConsoleAddress(), th);
        }
    }

    private void incrIsolatedMismatchIfNeeded(boolean mayIsolated) {
        if (mayIsolated != dcIsolated.get()) {
            int after = isolatedContinuousMismatchTimes.incrementAndGet();
            logger.info("[incrIsolatedMismatchIfNeeded] may {}:{}", mayIsolated ? "isolated":"unisolated", after);
        } else if (isolatedContinuousMismatchTimes.get() > 0) {
            logger.info("[incrIsolatedMismatchIfNeeded][reset]");
            this.isolatedContinuousMismatchTimes.set(0);
        }
    }

    private void toggleDcIsolatedIfNeeded() {
        if (dcIsolated.get() && isolatedContinuousMismatchTimes.get() >= config.getStableRecoverAfterRounds()) {
            logger.info("[toggleDcIsolatedIfNeeded] become unisolated");
            setDcIsolated(false);
        } else if (!dcIsolated.get() && isolatedContinuousMismatchTimes.get() > 0) {
            logger.info("[toggleDcIsolatedIfNeeded] become isolated");
            setDcIsolated(true);
        }
    }


    @VisibleForTesting
    protected void setSiteStable(boolean siteStable) {
        this.siteStable.set(siteStable);
        this.continuousMismatchTimes.set(0);
    }

    @VisibleForTesting
    protected void setDcIsolated(boolean isolated) {
        this.dcIsolated.set(isolated);
        this.isolatedContinuousMismatchTimes.set(0);
    }

    @Override
    protected void doInitialize() throws Exception {
        super.doInitialize();
        this.scheduled = Executors.newScheduledThreadPool(1, XpipeThreadFactory.create("StabilityInspector"));
        this.task = new DynamicDelayPeriodTask("StabilityInspector", new AbstractExceptionLogTask() {
            @Override
            protected void doRun() throws Exception {
                inspect();
            }
        }, config::getRedisReplicationHealthCheckInterval, scheduled);
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        this.task.start();
    }

    @Override
    protected void doStop() throws Exception {
        super.doStop();
        this.task.stop();
    }

    @Override
    protected void doDispose() throws Exception {
        super.doDispose();
        this.scheduled.shutdown();
        this.scheduled = null;
        this.task = null;
    }

    @Override
    public int getOrder() {
        return LOWEST_PRECEDENCE;
    }

    @Autowired
    public void setCollector(DefaultDelayPingActionCollector collector) {
        this.collector = collector;
    }

    @Autowired
    public void setMetaCache(MetaCache metaCache) {
        this.metaCache = metaCache;
    }

    @Autowired
    public void setConfig(CheckerConfig config) {
        this.config = config;
    }
}
