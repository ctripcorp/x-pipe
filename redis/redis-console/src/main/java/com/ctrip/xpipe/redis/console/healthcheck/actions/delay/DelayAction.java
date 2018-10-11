package com.ctrip.xpipe.redis.console.healthcheck.actions.delay;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.console.healthcheck.AbstractHealthCheckAction;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.actions.interaction.HealthStatus;
import com.ctrip.xpipe.redis.console.healthcheck.actions.ping.PingService;
import com.ctrip.xpipe.redis.console.healthcheck.session.RedisSession;
import com.ctrip.xpipe.utils.DateTimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
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
        reportDelay();
        RedisSession session = instance.getRedisSession();
        session.subscribeIfAbsent(CHECK_CHANNEL, callback);
        if(instance.getRedisInstanceInfo().isMaster()) {
            session.publish(CHECK_CHANNEL, Long.toHexString(System.nanoTime()));
        }
    }

    private void reportDelay() {
        if(INIT_CONTEXT.equals(context.get())) {
            return;
        }
        if(isExpired()) {
            logger.warn("[expire][{}] last update time: {}", instance.getRedisInstanceInfo().getHostPort(),
                    DateTimeUtils.timeAsString(context.get().getRecvTimeMilli()));

            long result = SAMPLE_LOST_AND_NO_PONG;
            if(pingService.isRedisAlive(instance.getRedisInstanceInfo().getHostPort())) {
                result = SAMPLE_LOST_BUT_PONG;
            }
            notifyListeners(new DelayActionContext(instance, result));
        } else {
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

    private void onMessage(String message) {
        long currentTime = System.nanoTime();
        long lastDelayPubTimeNano = Long.parseLong(message, 16);
        this.context.set(new DelayActionContext(instance, currentTime - lastDelayPubTimeNano));
    }

    private boolean isExpired() {
        long lastDelay = System.currentTimeMillis() - context.get().getRecvTimeMilli();
        return lastDelay >= expireInterval;
    }

}
