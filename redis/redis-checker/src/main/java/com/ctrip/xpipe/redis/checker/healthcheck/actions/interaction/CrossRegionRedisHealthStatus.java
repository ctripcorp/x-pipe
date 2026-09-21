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
 *  pingSuccess -> INSTANCEUP
 *  pingFail -> DOWN + markDown
 *  subSuccess -> ignored，需先 ping 通
 * <p>
 * INSTANCEUP
 *  pingSuccess,do nothing
 *  pingFail -> DOWN + markDown
 *  subSuccess -> HEALTHY + markUp
 * <p>
 * HEALTHY
 *  pingSuccess,do nothing
 *  pingFail -> DOWN + markDown
 *  subSuccess -> ignored
 * <p>
 * DOWN
 *  pingSuccess -> INSTANCEUP
 *  pingFail,do nothing
 *  subSuccess -> ignored，需先 ping 通
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
            notify(new InstanceLoading(instance));
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

    /** PsubAction 收到 xpipe* 拉入通知后写回 */
    @Override
    protected void subSuccess() {
        HEALTH_STATE preState = state.get();
        if (preState.equals(HEALTH_STATE.INSTANCEUP)) {
            if(state.compareAndSet(preState, HEALTH_STATE.HEALTHY)) {
                logStateChange(preState, state.get());
            }
            logger.info("[setUp] {}", this);
            notify(new InstanceUp(instance));
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
            notify(new InstanceDown(instance));
        }
    }

}
