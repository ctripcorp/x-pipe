package com.ctrip.xpipe.redis.checker.healthcheck.actions.keeperdelay;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.AbstractHealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.KeeperHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.delay.DelayConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HealthStatus;
import com.ctrip.xpipe.redis.checker.healthcheck.capability.KeeperCapabilityCache;
import com.ctrip.xpipe.redis.checker.healthcheck.keeperstate.KeeperStateCache;
import com.ctrip.xpipe.redis.checker.healthcheck.session.Callbackable;
import com.ctrip.xpipe.redis.checker.healthcheck.session.RedisSession;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import com.ctrip.xpipe.utils.DateTimeUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

public class KeeperDelayAction extends AbstractHealthCheckAction<KeeperHealthCheckInstance> {

    private static final Logger logger = LoggerFactory.getLogger(KeeperDelayAction.class);
    protected static final KeeperDelayActionContext INIT_CONTEXT =
            new KeeperDelayActionContext(null, HealthStatus.UNSET_TIME);
    public static final long SAMPLE_LOST = 99999L * 1000 * 1000;

    private final CheckerConfig checkerConfig;
    private final KeeperCapabilityCache capabilityCache;
    private final KeeperStateCache keeperStateCache;
    private final AtomicReference<KeeperDelayActionContext> context = new AtomicReference<>(INIT_CONTEXT);
    /** 最近一次 INFO 的结果；采集失败置 null，不再声称知道角色。 */
    private final AtomicReference<KeeperState> keeperState = new AtomicReference<>();
    /** 已发出的 INFO 采集序号，用于丢弃晚到的响应。 */
    private final AtomicLong keeperStateAttempt = new AtomicLong();
    private final LongSupplier expireInterval;
    private final String[] subscribeChannels;
    private final Object subscriptionLock = new Object();
    private RedisSession.SubscribeCallback callback;
    private long subscriptionGeneration;
    private long keeperStateAttemptApplied;
    private boolean contextInitialized;
    private boolean expired;
    private boolean subscriptionRequested;
    private boolean stoppingOrStopped;

    public KeeperDelayAction(ScheduledExecutorService scheduled, KeeperHealthCheckInstance instance,
                             ExecutorService executors, FoundationService foundationService,
                             CheckerConfig checkerConfig, KeeperCapabilityCache capabilityCache,
                             KeeperStateCache keeperStateCache) {
        super(scheduled, instance, executors);
        this.checkerConfig = checkerConfig;
        this.capabilityCache = capabilityCache;
        this.keeperStateCache = keeperStateCache;
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
        long generation;
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
            generation = subscriptionGeneration;
        }
        // 在锁外发起：INFO 的回调要抢同一把锁，若在锁内发出，一个就地完成的失败回调会让
        // netty 线程在这里等锁
        requestKeeperState(generation);
    }

    /**
     * 发起一次 INFO replication 取实时 ACTIVE / BACKUP。异步发出、回调里才写值，
     * 因此不会在 {@link #subscriptionLock} 上等待网络；本轮上报用的仍是上一轮采到的状态。
     */
    private void requestKeeperState(long generation) {
        long attempt = keeperStateAttempt.incrementAndGet();
        instance.getRedisSession().infoReplication(new Callbackable<String>() {
            @Override
            public void success(String info) {
                onKeeperState(generation, attempt, parseKeeperState(info));
            }

            @Override
            public void fail(Throwable throwable) {
                logger.debug("[requestKeeperState][{}] info replication failed",
                        instance.getCheckInfo().getHostPort(), throwable);
                // 问不通就别再声称知道它的角色；INFO 有命令超时兜底，这个回调一定会到
                onKeeperState(generation, attempt, null);
            }
        });
    }

    /**
     * @param attempt 本次采集的序号。晚到的响应不得覆盖更新的结果 —— 一次超时的失败回调可能排在
     *                下一轮的成功回调之后，若不比较序号就会把刚采到的状态抹成 unknown。
     *                generation 挡不住这种情况：同一轮订阅内它是不变的。
     */
    private void onKeeperState(long generation, long attempt, KeeperState state) {
        synchronized (subscriptionLock) {
            if (stoppingOrStopped || generation != subscriptionGeneration || !getLifecycleState().isStarted()) {
                return;
            }
            if (attempt <= keeperStateAttemptApplied) {
                return;
            }
            keeperStateAttemptApplied = attempt;
            keeperState.set(state);
            if (null == state) {
                keeperStateCache.remove(instance.getCheckInfo().getHostPort());
            } else {
                keeperStateCache.update(instance.getCheckInfo().getHostPort(), state);
            }
        }
    }

    /** Keeper 回的 {@code state:} 取值域是 {@link KeeperState}，但未知值不应让采集抛异常。 */
    private KeeperState parseKeeperState(String info) {
        String raw = new InfoResultExtractor(info).getKeeperState();
        if (null == raw) {
            return null;
        }
        try {
            return KeeperState.valueOf(raw.trim());
        } catch (IllegalArgumentException e) {
            logger.info("[parseKeeperState][{}] unknown keeper state: {}",
                    instance.getCheckInfo().getHostPort(), raw);
            return null;
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
                notifyListeners(new KeeperDelayActionContext(instance, SAMPLE_LOST, keeperState.get()));
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
            // 原样转发，保留样本到达时的 recvTimeMilli（打点以它为时间戳）
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
                    System.nanoTime() - Long.parseLong(message, 16), keeperState.get()));
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
        // 门禁关闭或降级后不再采集，陈旧的 ACTIVE 标记必须撤掉，否则打点侧会据此误判链路磁盘类型
        keeperState.set(null);
        // 同时放行后续采集：恢复订阅后的首次响应序号必然大于此处记录的值
        keeperStateAttemptApplied = keeperStateAttempt.get();
        keeperStateCache.remove(instance.getCheckInfo().getHostPort());
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
