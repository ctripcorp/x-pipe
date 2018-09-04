package com.ctrip.xpipe.redis.console.healthcheck.delay;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.redis.console.health.RedisSession;
import com.ctrip.xpipe.redis.console.healthcheck.BaseContext;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckContext;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.action.DelayHealthStatus;
import com.ctrip.xpipe.redis.console.healthcheck.action.HEALTH_STATE;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author chen.zhu
 * <p>
 * Aug 28, 2018
 */
public class DefaultDelayContext extends BaseContext implements DelayContext {

    public static final String CHECK_CHANNEL = "xpipe-health-check-" + FoundationService.DEFAULT.getLocalIp();

    private SubscribeCallback callback = new SubscribeCallback();

    private volatile long lastTimeDelay = HealthCheckContext.TIME_UNSET;

    private volatile long lastDelayNano = HealthCheckContext.TIME_UNSET;

    private volatile long lastDelayPubTimeNano = HealthCheckContext.TIME_UNSET;

    private ExecutorService executors;

    private List<DelayCollector> collectors;

    private DelayHealthStatus delayHealthStatus;

    public DefaultDelayContext(ScheduledExecutorService scheduled, RedisHealthCheckInstance instance,
                               ExecutorService executors, List<DelayCollector> collectors) {
        super(scheduled, instance);
        this.executors = executors;
        this.collectors = collectors;
    }

    @Override
    public long lastTimeDelayMilli() {
        return lastTimeDelay;
    }

    @Override
    public long lastDelayNano() {
        return lastDelayNano;
    }

    @Override
    public long lastDelayPubTimeNano() {
        return lastDelayPubTimeNano;
    }

    @Override
    public boolean isHealthy() {
        return delayHealthStatus.getState().equals(HEALTH_STATE.UP);
    }

    @Override
    public HEALTH_STATE getHealthState() {
        return delayHealthStatus.getState();
    }

    @Override
    protected void doScheduledTask() {
        recordIfExpired();
        RedisSession session = instance.getRedisSession();
        session.subscribeIfAbsent(CHECK_CHANNEL, callback);
        if(instance.getHealthCheckContext().getRedisContext().isMater()) {
            logger.info("[doScheduledTask] master endpoint {}, pub message", instance.getEndpoint());
            session.publish(CHECK_CHANNEL, Long.toHexString(System.nanoTime()));
        }
    }

    @Override
    protected void doInitialize() throws Exception {
        super.doInitialize();
        scheduled.schedule(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() {
                instance.getRedisSession().subscribeIfAbsent(CHECK_CHANNEL, callback);
            }
        }, getWarmupTime(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void doStart() throws Exception {
        super.doStart();
        delayHealthStatus = new DelayHealthStatus(instance, scheduled);
    }

    @Override
    public void doStop() throws Exception {
        LifecycleHelper.stopIfPossible(delayHealthStatus);
        super.doStop();
    }

    private void recordIfExpired() {
        int expireTime = instance.getHealthCheckConfig().getHealthyDelayMilli();
        boolean expired = System.currentTimeMillis() - lastTimeDelayMilli() >= expireTime;
        if(expired) {
            if(instance.getHealthCheckContext().getPingContext().isHealthy()) {
                lastDelayNano = DelayContext.SAMPLE_LOST_BUT_PONG;
            } else {
                lastDelayNano = DelayContext.SAMPLE_LOST_AND_NO_PONG;
            }
            lastDelayPubTimeNano = System.nanoTime();
        }
        notifyCollectors();
    }

    private class SubscribeCallback implements RedisSession.SubscribeCallback {

        @Override
        public void message(String channel, String message) {
            doReceiveSub(message);
        }

        @Override
        public void fail(Throwable e) {
            //ignore sub fail
        }
    }

    private void notifyCollectors() {
        logger.info("[notifyCollectors] {}", collectors);
        for(DelayCollector collector : collectors) {
            executors.execute(new AbstractExceptionLogTask() {
                @Override
                protected void doRun() throws Exception {
                    collector.collect(instance);
                }
            });
        }
    }

    private void doReceiveSub(String message) {
        long currentTime = System.nanoTime();
        lastDelayPubTimeNano = Long.parseLong(message, 16);
        lastTimeDelay = System.currentTimeMillis();
        lastDelayNano = currentTime - lastDelayPubTimeNano;
        executors.execute(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() throws Exception {
                delayHealthStatus.delay(lastDelayNano);
                notifyCollectors();
            }
        });
    }
}
