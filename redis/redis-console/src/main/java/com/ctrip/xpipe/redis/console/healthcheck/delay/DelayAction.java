package com.ctrip.xpipe.redis.console.healthcheck.delay;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.console.health.RedisSession;
import com.ctrip.xpipe.redis.console.healthcheck.AbstractHealthCheckAction;
import com.ctrip.xpipe.redis.console.healthcheck.ActionContext;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.action.HealthStatus;
import com.ctrip.xpipe.redis.console.healthcheck.ping.PingService;
import com.ctrip.xpipe.utils.DateTimeUtils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author chen.zhu
 * <p>
 * Sep 06, 2018
 */
public class DelayAction extends AbstractHealthCheckAction<DelayActionContext> {

    public static final String CHECK_CHANNEL = "xpipe-health-check-" + FoundationService.DEFAULT.getLocalIp();

    public static final long SAMPLE_LOST_AND_NO_PONG = -99999L * 1000 * 1000;

    public static final long SAMPLE_LOST_BUT_PONG = 99999L * 1000 * 1000;

    private SubscribeCallback callback = new SubscribeCallback();

    private AtomicLong updated = new AtomicLong(-1L);

    private PingService pingService;

    public DelayAction(ScheduledExecutorService scheduled, RedisHealthCheckInstance instance,
                       ExecutorService executors, PingService pingService) {
        super(scheduled, instance, executors);
        this.pingService = pingService;
    }

    @Override
    protected void doScheduledTask() {
        markExpiration();
        RedisSession session = instance.getRedisSession();
        session.subscribeIfAbsent(CHECK_CHANNEL, callback);
        String message = Long.toHexString(System.nanoTime());
        if(instance.getRedisInstanceInfo().isMaster()) {
            logger.debug("[doScheduledTask] master endpoint {}, pub message", instance.getEndpoint());
            session.publish(CHECK_CHANNEL, message);
        }
    }

    private void markExpiration() {
        if(updated.get() == HealthStatus.UNSET_TIME) {
            return;
        }
        long expireInterval = instance.getHealthCheckConfig().getHealthyDelayMilli() + DELTA * 2;
        if(System.currentTimeMillis() - updated.get() >= expireInterval) {
            logger.info("[expire][{}] last update time: {}", instance.getRedisInstanceInfo().getHostPort(), DateTimeUtils.timeAsString(updated.get()));
            long result = SAMPLE_LOST_AND_NO_PONG;
            if(pingService.isRedisAlive(instance.getRedisInstanceInfo().getHostPort())) {
                result = SAMPLE_LOST_BUT_PONG;
            }
            notifyListeners(new DelayActionContext(instance, result));
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
        updated.set(System.currentTimeMillis());
        long currentTime = System.nanoTime();
        long lastDelayPubTimeNano = Long.parseLong(message, 16);
        DelayActionContext context = new DelayActionContext(instance, currentTime - lastDelayPubTimeNano);
        notifyListeners(context);
    }

}
