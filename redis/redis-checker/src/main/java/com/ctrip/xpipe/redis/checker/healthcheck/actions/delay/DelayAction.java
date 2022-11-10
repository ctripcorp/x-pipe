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

    protected static final Logger logger = LoggerFactory.getLogger(DelayAction.class);

    protected static final DelayActionContext INIT_CONTEXT = new DelayActionContext(null, HealthStatus.UNSET_TIME);

    public static final long SAMPLE_LOST_AND_NO_PONG = -99999L * 1000 * 1000;

    public static final long SAMPLE_LOST_BUT_PONG = 99999L * 1000 * 1000;

    private SubscribeCallback callback = new SubscribeCallback();

    protected AtomicReference<DelayActionContext> context = new AtomicReference<>(INIT_CONTEXT);

    private volatile boolean isExpired = false;

    private PingService pingService;

    protected final long expireInterval;

    private volatile boolean isContextInited = false;

    private String currentDcId;
    
    private String publish_channel;

    private String[] subscribe_channel;

    protected FoundationService foundationService;

    public DelayAction(ScheduledExecutorService scheduled, RedisHealthCheckInstance instance,
                       ExecutorService executors, PingService pingService, FoundationService foundationService) {
        super(scheduled, instance, executors);
        this.pingService = pingService;
        this.foundationService = foundationService;
        expireInterval = instance.getHealthCheckConfig().getHealthyDelayMilli() + DELTA * 2;
        this.currentDcId = foundationService.getDataCenter();
        this.publish_channel = "xpipe-health-check-" + foundationService.getLocalIp() + "-" + instance.getCheckInfo().getShardDbId();
        this.subscribe_channel = getSubscribeChannel();
    }

    @Override
    protected void doTask() {
        //TODO: log only when it's too long to not execute
//        logger.info("[doTask][begin][{}]", instance.getCheckInfo().getClusterShardHostport());
        reportDelay();
        RedisSession session = instance.getRedisSession();
        doSubscribe(session, callback, subscribe_channel);

        RedisInstanceInfo info = instance.getCheckInfo();
        if (currentDcId.equalsIgnoreCase(info.getDcId()) && info.isMaster()) {
            doPublish(session, publish_channel, Long.toHexString(System.nanoTime()));
        }
    }

    protected void doSubscribe(RedisSession session, SubscribeCallback callback, String... channel) {
        session.subscribeIfAbsent(callback, channel);
    }

    protected void doPublish(RedisSession session, String channel, String message) {
        session.publish(channel, message);
    }

    protected String[] getSubscribeChannel() {
        return new String[]{publish_channel};
    }

    @Override
    protected Logger getHealthCheckLogger() {
        return logger;
    }

    protected void reportDelay() {
        if (INIT_CONTEXT.equals(context.get()) && !isContextInited) {
            isContextInited = true;
            return;
        }
        if (isExpired(context.get())) {
            if (!isExpired) {
                isExpired = true;
                logger.warn("[expire][{}] last update time: {}", instance.getCheckInfo().getHostPort(),
                        DateTimeUtils.timeAsString(context.get().getRecvTimeMilli()));
            }

            onExpired(context.get());
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
            onNotExpired(context.get());
        }
    }

    protected void onExpired(DelayActionContext context) {
        long result = SAMPLE_LOST_AND_NO_PONG;
        if(pingService.isRedisAlive(instance.getCheckInfo().getHostPort())) {
            result = SAMPLE_LOST_BUT_PONG;
        }
        notifyListeners(new DelayActionContext(instance, result));
    }

    protected void onNotExpired(DelayActionContext context) {
        notifyListeners(context);
    }

    protected class SubscribeCallback implements RedisSession.SubscribeCallback {

        @Override
        public void message(String channel, String message) {
            onMessage(channel, message);
        }

        @Override
        public void fail(Throwable e) {
            //ignore sub fail
        }
    }

    @VisibleForTesting
    protected void onMessage(String channel, String message) {
        if (!getLifecycleState().isStarted()) {
            return;
        }
        long currentTime = System.nanoTime();
        long lastDelayPubTimeNano = Long.parseLong(message, 16);
        this.context.set(new DelayActionContext(instance, currentTime - lastDelayPubTimeNano));
    }

    @Override
    public void doStop() {
        instance.getRedisSession().closeSubscribedChannel(subscribe_channel);
        super.doStop();
    }

    protected boolean isExpired(DelayActionContext context) {
        long lastDelay = System.currentTimeMillis() - context.getRecvTimeMilli();
        return lastDelay >= expireInterval;
    }

}
