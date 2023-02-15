package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction;

import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.event.HeteroInstanceLongDelay;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.event.InstanceUp;
import com.google.common.annotations.VisibleForTesting;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class HeteroHealthStatus extends HealthStatus {

    private Map<Long, AtomicLong> lastHealthDelayTime = new ConcurrentHashMap<>();
    private Map<Long, AtomicReference<HEALTH_STATE>> state = new ConcurrentHashMap<>();

    public HeteroHealthStatus(RedisHealthCheckInstance instance, ScheduledExecutorService scheduled) {
        super(instance, scheduled);
    }

    void delay(long delayMilli, long... srcShardDbId) {
        if (lastHealthDelayTime.get(srcShardDbId[0]) == null)
            lastHealthDelayTime.put(srcShardDbId[0], new AtomicLong(System.currentTimeMillis()));

        delayLogger.debug("{}, {}", instance.getCheckInfo().getHostPort(), delayMilli);
        if (delayMilli >= 0 && delayMilli <= healthyDelayMilli.getAsInt()) {
            lastHealthDelayTime.put(srcShardDbId[0], new AtomicLong(System.currentTimeMillis()));
            setDelayUp(srcShardDbId[0]);
        }
    }

    private void setDelayUp(long srcShardDbId) {
        if (state.get(srcShardDbId) == null) {
            state.put(srcShardDbId, new AtomicReference<>(HEALTH_STATE.INSTANCEUP));
            logAndNotifyUp(srcShardDbId, HEALTH_STATE.UNKNOWN, HEALTH_STATE.INSTANCEUP);
        } else if (state.get(srcShardDbId).compareAndSet(HEALTH_STATE.UNHEALTHY, HEALTH_STATE.INSTANCEUP)) {
            logAndNotifyUp(srcShardDbId, HEALTH_STATE.UNHEALTHY, HEALTH_STATE.INSTANCEUP);
        }
    }

    private void logAndNotifyUp(long srcShardDbId, HEALTH_STATE pre, HEALTH_STATE cur) {
        logger.info("[setDelayUp]{} {}->{}", srcShardDbId, pre, cur);
        notifyObservers(new InstanceUp(instance));
    }

    protected boolean shouldNotRun() {
        return lastHealthDelayTime.isEmpty();
    }

    protected void healthStatusUpdate() {
        long currentTime = System.currentTimeMillis();
        int instanceLongDelay = instanceLongDelayMilli.getAsInt();
        for (long srcShardDbId : lastHealthDelayTime.keySet()) {
            long delayDownTime = currentTime - lastHealthDelayTime.get(srcShardDbId).get();
            if(delayDownTime >= instanceLongDelay) {
                setDelayHalfDown(srcShardDbId);
            }
        }
    }

    private void setDelayHalfDown(long srcShardDbId) {
        if (state.get(srcShardDbId) == null) {
            state.put(srcShardDbId, new AtomicReference<>(HEALTH_STATE.UNHEALTHY));
            logAndNotifyHalfDown(srcShardDbId, HEALTH_STATE.UNKNOWN, HEALTH_STATE.UNHEALTHY);
        } else if (state.get(srcShardDbId).compareAndSet(HEALTH_STATE.INSTANCEUP, HEALTH_STATE.UNHEALTHY)) {
            logAndNotifyHalfDown(srcShardDbId, HEALTH_STATE.INSTANCEUP, HEALTH_STATE.UNHEALTHY);
        }
    }

    private void logAndNotifyHalfDown(long srcShardDbId, HEALTH_STATE pre, HEALTH_STATE cur) {
        logger.info("[setDelayHalfDown]{} {}->{}", srcShardDbId, pre, cur);
        notifyObservers(new HeteroInstanceLongDelay(instance, srcShardDbId));
    }

    @VisibleForTesting
    HEALTH_STATE getState(long srcShardDbId) {
        if (lastHealthDelayTime.get(srcShardDbId) == null)
            return null;

        if (state.get(srcShardDbId) == null)
            return HEALTH_STATE.UNKNOWN;

        return state.get(srcShardDbId).get();
    }

    @VisibleForTesting
    Long getLastHealthDelayTime(long srcShardDbId) {
        if (lastHealthDelayTime.get(srcShardDbId) == null)
            return null;

        return lastHealthDelayTime.get(srcShardDbId).get();
    }

}
