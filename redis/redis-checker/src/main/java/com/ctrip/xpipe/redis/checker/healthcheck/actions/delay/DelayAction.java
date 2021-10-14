package com.ctrip.xpipe.redis.checker.healthcheck.actions.delay;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.checker.healthcheck.AbstractHealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HealthStatus;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.ping.PingService;
import com.ctrip.xpipe.redis.checker.healthcheck.session.RedisSession;
import com.ctrip.xpipe.utils.DateTimeUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;
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
public class DelayAction extends AbstractHealthCheckAction<RedisHealthCheckInstance> {

    private static final Logger logger = LoggerFactory.getLogger(DelayAction.class);

    private static final DelayActionContext INIT_CONTEXT = new DelayActionContext(null, HealthStatus.UNSET_TIME);

    public static final long SAMPLE_LOST_AND_NO_PONG = -99999L * 1000 * 1000;

    public static final long SAMPLE_LOST_BUT_PONG = 99999L * 1000 * 1000;

    private SubscribeCallback callback = new SubscribeCallback();

    protected AtomicReference<DelayActionContext> context = new AtomicReference<>(INIT_CONTEXT);

    private volatile boolean isExpired = false;

    private PingService pingService;

    private final long expireInterval;

    private volatile boolean isContextInited = false;

    private String currentDcId;
    
    private String channel;

    public DelayAction(ScheduledExecutorService scheduled, RedisHealthCheckInstance instance,
                       ExecutorService executors, PingService pingService, FoundationService foundationService) {
        super(scheduled, instance, executors);
        this.pingService = pingService;
        expireInterval = instance.getHealthCheckConfig().getHealthyDelayMilli() + DELTA * 2;
        this.currentDcId = foundationService.getDataCenter();
        this.channel =  "xpipe-health-check-" + foundationService.getLocalIp();

    }

    @Override
    protected void doTask() {
        //TODO: log only when it's too long to not execute
//        logger.info("[doTask][begin][{}]", instance.getCheckInfo().getClusterShardHostport());
        reportDelay();
        RedisSession session = instance.getRedisSession();
        doSubscribe(session, channel, callback);

        RedisInstanceInfo info = instance.getCheckInfo();
        if (currentDcId.equalsIgnoreCase(info.getDcId()) && info.isMaster()) {
//            logger.info("[doTask][pub][{}]", instance.getCheckInfo().getClusterShardHostport());
            doPublish(session, channel, Long.toHexString(System.nanoTime()));
        }
    }

    protected void doSubscribe(RedisSession session, String channel, SubscribeCallback callback) {
        session.subscribeIfAbsent(channel, callback);
    }

    protected void doPublish(RedisSession session, String channel, String message) {
        session.publish(channel, message);
    }

    @Override
    protected Logger getHealthCheckLogger() {
        return logger;
    }

    private void reportDelay() {
        if(INIT_CONTEXT.equals(context.get()) && !isContextInited) {
            isContextInited = true;
            return;
        }
        if(isExpired()) {
            if (!isExpired) {
                isExpired = true;
                logger.warn("[expire][{}] last update time: {}", instance.getCheckInfo().getHostPort(),
                        DateTimeUtils.timeAsString(context.get().getRecvTimeMilli()));
            }

            onExpired();
        } else {
            if (INIT_CONTEXT.equals(context.get())) {
                // no receive any messages but not expire just on init time
                logger.info("[expire][{}] init but not expire", instance.getCheckInfo().getHostPort());
                return;
            }
            if (isExpired) {
                isExpired = false;
                logger.info("[expire][{}] recovery", instance.getCheckInfo().getHostPort());
            }
            onNotExpired();
        }
    }

    protected void onExpired() {
        long result = SAMPLE_LOST_AND_NO_PONG;
        if(pingService.isRedisAlive(instance.getCheckInfo().getHostPort())) {
            result = SAMPLE_LOST_BUT_PONG;
        }
        notifyListeners(new DelayActionContext(instance, result));
    }

    protected void onNotExpired() {
        notifyListeners(context.get());
    }

    protected class SubscribeCallback implements RedisSession.SubscribeCallback {

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
        instance.getRedisSession().closeSubscribedChannel(channel);
        super.doStop();
    }

    private boolean isExpired() {
        long lastDelay = System.currentTimeMillis() - context.get().getRecvTimeMilli();
        return lastDelay >= expireInterval;
    }

}
