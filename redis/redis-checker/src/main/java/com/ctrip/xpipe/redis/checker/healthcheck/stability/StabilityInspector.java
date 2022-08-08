package com.ctrip.xpipe.redis.checker.healthcheck.stability;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
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

    private AtomicBoolean stable;

    private AtomicInteger continuousMismatchTimes;

    private AtomicInteger continueNoInterested;

    private DynamicDelayPeriodTask task;

    private ScheduledExecutorService scheduled;

    private static final String TYPE = "stability";

    @Autowired
    public StabilityInspector(DefaultDelayPingActionCollector defaultDelayPingActionCollector, MetaCache metaCache,
                              CheckerConfig checkerConfig) {
        this.collector = defaultDelayPingActionCollector;
        this.metaCache = metaCache;
        this.config = checkerConfig;
        this.stable = new AtomicBoolean(true);
        this.continuousMismatchTimes = new AtomicInteger();
        this.continueNoInterested = new AtomicInteger();
    }

    protected boolean isSiteStable() {
        return stable.get();
    }

    @VisibleForTesting
    protected void inspect() {
        logger.debug("[inspect] begin");

        Map<HostPort, HEALTH_STATE> currentAllHealthStates = collector.getAllCachedState();
        String currentDc = FoundationService.DEFAULT.getDataCenter();
        Set<HostPort> interested = currentAllHealthStates.keySet().stream()
                .filter(hostPort -> {
                    try {
                        return currentDc.equalsIgnoreCase(metaCache.getDc(hostPort));
                    } catch (Throwable th) {
                        logger.debug("[inspect][ignore instance] {}", hostPort, th);
                        return false;
                    }
                }).collect(Collectors.toSet());

        if (interested.isEmpty()) {
            continueNoInterested.incrementAndGet();
        } else {
            int upCnt = 0;
            int downCnt = 0;
            for (HostPort hostPort : interested) {
                HEALTH_STATE healthState = currentAllHealthStates.get(hostPort);
                if (HEALTH_STATE.HEALTHY.equals(healthState)) upCnt++;
                if (HEALTH_STATE.DOWN.equals(healthState)) downCnt++;
            }

            boolean mayStable = stable.get();
            if ((upCnt * 1.0/interested.size()) > config.getSiteStableThreshold()) {
                mayStable = true;
            } else if ((downCnt * 1.0/interested.size()) > config.getSiteUnstableThreshold()) {
                mayStable = false;
            }

            incrMismatchIfNeeded(mayStable);
        }

        toggleStableIfNeeded();
        EventMonitor.DEFAULT.logEvent(TYPE, stable.get() ? "stable" : "unstable");
    }

    private void incrMismatchIfNeeded(boolean mayStable) {
        continueNoInterested.set(0);
        if (mayStable != stable.get()) {
            int after = continuousMismatchTimes.incrementAndGet();
            logger.info("[incrMismatchIfNeeded] may {}:{}", mayStable ? "stable":"unstable", after);
        } else {
            logger.info("[incrMismatchIfNeeded][reset]");
            this.continuousMismatchTimes.set(0);
        }
    }

    private void toggleStableIfNeeded() {
        if (stable.get() && continuousMismatchTimes.get() >= config.getStableLossAfterRounds()) {
            logger.info("[toggleStableIfNeeded] become unstable");
            stable.set(false);
        } else if (!stable.get() && continuousMismatchTimes.get() >= config.getStableRecoverAfterRounds()) {
            logger.info("[toggleStableIfNeeded] become stable");
            stable.set(true);
        } else if (!stable.get() && continueNoInterested.get() >= config.getStableResetAfterRounds()) {
            logger.info("[toggleStableIfNeeded][continue no interested] become stable");
            stable.set(true);
        }
    }

    @VisibleForTesting
    protected void setStable(boolean stable) {
        this.stable.set(stable);
        this.continuousMismatchTimes.set(0);
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

}
