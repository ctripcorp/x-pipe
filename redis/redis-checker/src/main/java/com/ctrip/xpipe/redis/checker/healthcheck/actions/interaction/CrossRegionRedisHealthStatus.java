package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction;

import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.event.InstanceDown;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.event.InstanceLoading;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.event.InstanceUp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;

/**
 * UNKNOWN
 *  pingSuccess -> INSTANCEUP + start subAction
 *  pingFail -> DOWN + markDown
 *  subSuccess -> throw exception
 * <p>
 * INSTANCEUP
 *  pingSuccess,do nothing
 *  pingFail -> DOWN + markDown
 *  subSuccess -> HEALTHY + markUp + stop subAction
 * <p>
 * HEALTHY
 *  pingSuccess,do nothing
 *  pingFail -> DOWN + markDown
 *  subSuccess -> throw exception
 * <p>
 * DOWN
 *  pingSuccess -> INSTANCEUP + start subAction
 *  pingFail,do nothing
 *  subSuccess -> throw exception
 */
public class CrossRegionRedisHealthStatus extends HealthStatus {

    protected static final Logger logger = LoggerFactory.getLogger(CrossRegionRedisHealthStatus.class);

    public CrossRegionRedisHealthStatus(RedisHealthCheckInstance instance, ScheduledExecutorService scheduled) {
        super(instance, scheduled);
    }

    @Override
    protected void loading() {
        HEALTH_STATE preState = state.get();
        if(state.compareAndSet(preState, HEALTH_STATE.DOWN)) {
            logStateChange(preState, state.get());
        }
        if (!preState.equals(HEALTH_STATE.DOWN)) {
            logger.info("[setLoading] {}", this);
            notifyObservers(new InstanceLoading(instance));
        }
    }

    @Override
    protected void pong() {
        lastPongTime.set(System.currentTimeMillis());
        HEALTH_STATE preState = state.get();
        if (preState.equals(HEALTH_STATE.UNKNOWN) || preState.equals(HEALTH_STATE.DOWN)) {
            if(state.compareAndSet(preState, HEALTH_STATE.INSTANCEUP)) {
                logStateChange(preState, state.get());
            }
        }
    }

    @Override
    protected void subSuccess() {
        HEALTH_STATE preState = state.get();
        if (preState.equals(HEALTH_STATE.INSTANCEUP)) {
            if(state.compareAndSet(preState, HEALTH_STATE.HEALTHY)) {
                logStateChange(preState, state.get());
            }
            logger.info("[setUp] {}", this);
            notifyObservers(new InstanceUp(instance));
        }
    }

    @Override
    protected void healthStatusUpdate() {
        long currentTime = System.currentTimeMillis();

        if(lastPongTime.get() != UNSET_TIME) {
            long pingDownTime = currentTime - lastPongTime.get();
            final int pingDownAfter = pingDownAfterMilli.getAsInt();
            if (pingDownTime > pingDownAfter) {
                doMarkDown();
            }
        }
    }

    protected void doMarkDown() {
        HEALTH_STATE preState = state.get();
        if(state.compareAndSet(preState, HEALTH_STATE.DOWN)) {
            logStateChange(preState, state.get());
        }
        if (!preState.equals(HEALTH_STATE.DOWN)) {
            logger.info("[setDown] {}", this);
            notifyObservers(new InstanceDown(instance));
        }
    }

}
