package com.ctrip.xpipe.redis.console.healthcheck.actions.delay;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.console.healthcheck.AbstractHealthCheckAction;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.actions.interaction.HealthStatus;
import com.ctrip.xpipe.redis.console.healthcheck.actions.ping.PingService;
import com.ctrip.xpipe.redis.console.healthcheck.session.RedisSession;
import com.ctrip.xpipe.utils.DateTimeUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author chen.zhu
 * <p>
 * Sep 06, 2018
 */
public class DelayAction extends AbstractHealthCheckAction<DelayActionContext> {

    private static final Logger logger = LoggerFactory.getLogger(DelayAction.class);

    public static final String CHECK_CHANNEL = "xpipe-health-check-" + FoundationService.DEFAULT.getLocalIp();

    private static final DelayActionContext INIT_CONTEXT = new DelayActionContext(null, HealthStatus.UNSET_TIME);

    public static final long SAMPLE_LOST_AND_NO_PONG = -99999L * 1000 * 1000;

    public static final long SAMPLE_LOST_BUT_PONG = 99999L * 1000 * 1000;

    private SubscribeCallback callback = new SubscribeCallback();

    private AtomicReference<DelayActionContext> context = new AtomicReference<>(INIT_CONTEXT);

    private AtomicInteger continuouslyDelayCnt = new AtomicInteger(0);

    protected static int EXPIRE_LOG_FREQUENCY = Integer.parseInt(System.getProperty("EXPIRE_LOG_FREQUENCY", "30"));

    private PingService pingService;

    private final long expireInterval;

    public DelayAction(ScheduledExecutorService scheduled, RedisHealthCheckInstance instance,
                       ExecutorService executors, PingService pingService) {
        super(scheduled, instance, executors);
        this.pingService = pingService;
        expireInterval = instance.getHealthCheckConfig().getHealthyDelayMilli() + DELTA * 2;
    }

    @Override
    protected void doTask() {
        //TODO: log only when it's too long to not execute
//        logger.info("[doTask][begin][{}]", instance.getRedisInstanceInfo().getClusterShardHostport());
        reportDelay();
        RedisSession session = instance.getRedisSession();
        session.subscribeIfAbsent(CHECK_CHANNEL, callback);
        if(instance.getRedisInstanceInfo().isMaster()) {
//            logger.info("[doTask][pub][{}]", instance.getRedisInstanceInfo().getClusterShardHostport());
            session.publish(CHECK_CHANNEL, Long.toHexString(System.nanoTime()));
        }
    }

    @Override
    protected Logger getHealthCheckLogger() {
        return logger;
    }

    private void reportDelay() {
        if(INIT_CONTEXT.equals(context.get())) {
            return;
        }
        if(isExpired()) {
            if (Math.abs(continuouslyDelayCnt.incrementAndGet()) % EXPIRE_LOG_FREQUENCY == 1) {
                logger.warn("[expire][{}] last update time: {}", instance.getRedisInstanceInfo().getHostPort(),
                        DateTimeUtils.timeAsString(context.get().getRecvTimeMilli()));
            }

            long result = SAMPLE_LOST_AND_NO_PONG;
            if(pingService.isRedisAlive(instance.getRedisInstanceInfo().getHostPort())) {
                result = SAMPLE_LOST_BUT_PONG;
            }
            notifyListeners(new DelayActionContext(instance, result));
        } else {
            if (continuouslyDelayCnt.get() != 0) {
                continuouslyDelayCnt.set(0);
                logger.info("[expire][{}] recovery", instance.getRedisInstanceInfo().getHostPort());
            }
            notifyListeners(context.get());
        }
    }

    private class SubscribeCallback implements RedisSession.SubscribeCallback {

        @Override
        public void message(String channel, String message) {
            onMessage(message);
        }

        @Override
        public void fail(Throwable e) {
            //ignore sub fail
        }
    }

    @VisibleForTesting
    protected void onMessage(String message) {
        if (!getLifecycleState().isStarted()) {
            return;
        }
        long currentTime = System.nanoTime();
        long lastDelayPubTimeNano = Long.parseLong(message, 16);
        this.context.set(new DelayActionContext(instance, currentTime - lastDelayPubTimeNano));
    }

    @Override
    public void doStop() {
        instance.getRedisSession().closeSubscribedChannel(CHECK_CHANNEL);
        super.doStop();
    }

    private boolean isExpired() {
        long lastDelay = System.currentTimeMillis() - context.get().getRecvTimeMilli();
        return lastDelay >= expireInterval;
    }

}
