package com.ctrip.xpipe.redis.checker.healthcheck.actions.keeperdelay;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.AbstractHealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.KeeperHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.delay.DelayConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HealthStatus;
import com.ctrip.xpipe.redis.checker.healthcheck.capability.KeeperCapabilityCache;
import com.ctrip.xpipe.redis.checker.healthcheck.session.RedisSession;
import com.ctrip.xpipe.utils.DateTimeUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

public class KeeperDelayAction extends AbstractHealthCheckAction<KeeperHealthCheckInstance> {

    private static final Logger logger = LoggerFactory.getLogger(KeeperDelayAction.class);
    protected static final KeeperDelayActionContext INIT_CONTEXT =
            new KeeperDelayActionContext(null, HealthStatus.UNSET_TIME);
    public static final long SAMPLE_LOST = 99999L * 1000 * 1000;

    private final CheckerConfig checkerConfig;
    private final KeeperCapabilityCache capabilityCache;
    private final AtomicReference<KeeperDelayActionContext> context = new AtomicReference<>(INIT_CONTEXT);
    private final LongSupplier expireInterval;
    private final String[] subscribeChannels;
    private final Object subscriptionLock = new Object();
    private RedisSession.SubscribeCallback callback;
    private long subscriptionGeneration;
    private boolean contextInitialized;
    private boolean expired;
    private boolean subscriptionRequested;
    private boolean stoppingOrStopped;

    public KeeperDelayAction(ScheduledExecutorService scheduled, KeeperHealthCheckInstance instance,
                             ExecutorService executors, FoundationService foundationService,
                             CheckerConfig checkerConfig, KeeperCapabilityCache capabilityCache) {
        super(scheduled, instance, executors);
        this.checkerConfig = checkerConfig;
        this.capabilityCache = capabilityCache;
        String currentDc = foundationService.getDataCenter();
        this.expireInterval = () -> {
            DelayConfig delayConfig = instance.getHealthCheckConfig().getDelayConfig(
                    instance.getCheckInfo().getClusterId(), currentDc, instance.getCheckInfo().getDcId());
            int clusterDelay = delayConfig.getClusterLevelHealthyDelayMilli();
            return (clusterDelay < 0 ? delayConfig.getDcLevelHealthyDelayMilli() : clusterDelay) + DELTA * 2;
        };
        this.subscribeChannels = new String[]{"xpipe-health-check-" + foundationService.getLocalIp()
                + "-" + instance.getCheckInfo().getShardDbId()};
    }

    @Override
    protected void doTask() {
        synchronized (subscriptionLock) {
            if (stoppingOrStopped) {
                return;
            }
            if (!checkerConfig.isKeeperDelayCheckEnabled()) {
                closeSubscription(false);
                return;
            }

            KeeperCapabilityCache.Capability capability =
                    capabilityCache.getIfPresent(instance.getCheckInfo().getHostPort());
            if (capability != KeeperCapabilityCache.Capability.SUPPORTED) {
                closeSubscription(false);
                return;
            }

            reportDelay();
            if (!subscriptionRequested) {
                callback = new SubscribeCallback(++subscriptionGeneration);
                subscriptionRequested = true;
            }
            instance.getRedisSession().subscribeIfAbsent(callback, subscribeChannels);
        }
    }

    private void reportDelay() {
        KeeperDelayActionContext current = context.get();
        if (current == INIT_CONTEXT && !contextInitialized) {
            contextInitialized = true;
            return;
        }
        if (isExpired(current)) {
            if (!expired) {
                expired = true;
                logger.warn("[expire][{}] last update time: {}", instance.getCheckInfo().getHostPort(),
                        DateTimeUtils.timeAsString(current.getRecvTimeMilli()));
            }
            if (checkerConfig.isKeeperDelayCheckEnabled()) {
                notifyListeners(new KeeperDelayActionContext(instance, SAMPLE_LOST));
            }
            return;
        }
        if (current == INIT_CONTEXT) {
            return;
        }
        if (expired) {
            expired = false;
            logger.info("[expire][{}] recovery", instance.getCheckInfo().getHostPort());
        }
        if (checkerConfig.isKeeperDelayCheckEnabled()) {
            notifyListeners(current);
        }
    }

    private boolean isExpired(KeeperDelayActionContext current) {
        return System.currentTimeMillis() - current.getRecvTimeMilli() >= expireInterval.getAsLong();
    }

    @VisibleForTesting
    String subscribeChannel() {
        return subscribeChannels[0];
    }

    private void onMessage(long generation, String message) {
        synchronized (subscriptionLock) {
            if (stoppingOrStopped || !subscriptionRequested || generation != subscriptionGeneration
                    || !getLifecycleState().isStarted()) {
                return;
            }
            context.set(new KeeperDelayActionContext(instance,
                    System.nanoTime() - Long.parseLong(message, 16)));
        }
    }

    private void closeSubscription(boolean force) {
        boolean shouldClose = force || subscriptionRequested;
        subscriptionRequested = false;
        callback = null;
        subscriptionGeneration++;
        context.set(INIT_CONTEXT);
        contextInitialized = false;
        expired = false;
        if (shouldClose) {
            instance.getRedisSession().closeSubscribedChannel(subscribeChannels);
        }
    }

    @Override
    public void doStop() {
        synchronized (subscriptionLock) {
            stoppingOrStopped = true;
            closeSubscription(true);
        }
        super.doStop();
    }

    @Override
    protected Logger getHealthCheckLogger() {
        return logger;
    }

    private class SubscribeCallback implements RedisSession.SubscribeCallback {

        private final long generation;

        private SubscribeCallback(long generation) {
            this.generation = generation;
        }

        @Override
        public void message(String channel, String message) {
            onMessage(generation, message);
        }

        @Override
        public void fail(Throwable e) {
            // A later check cycle retries subscribeIfAbsent.
        }
    }
}
