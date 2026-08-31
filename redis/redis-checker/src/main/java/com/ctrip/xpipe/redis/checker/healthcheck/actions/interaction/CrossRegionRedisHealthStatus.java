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
 * <p>
 * INSTANCEUP
 *  pingSuccess,do nothing
 *  pingFail -> DOWN + markDown
 *  replIdMatch -> HEALTHY + markUp
 * <p>
 * HEALTHY
 *  pingSuccess,do nothing
 *  pingFail -> DOWN + markDown
 * <p>
 * DOWN
 *  pingSuccess -> INSTANCEUP
 *  pingFail,do nothing
 */
public class CrossRegionRedisHealthStatus extends HealthStatus {

    protected static final Logger logger = LoggerFactory.getLogger(CrossRegionRedisHealthStatus.class);

    private volatile String slaveReplId;
    private volatile String keeperReplId;

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

    /** InfoReplIdAction 的 listener 写回；replId 一致时拉入 */
    public void updateReplIds(String slaveReplId, String keeperReplId) {
        this.slaveReplId = slaveReplId;
        this.keeperReplId = keeperReplId;
        if (replIdMatch()) {
            markUp();
        }
    }

    /** 校验逻辑：只比较 replId 是否相等 */
    public boolean replIdMatch() {
        return slaveReplId != null && keeperReplId != null && slaveReplId.equals(keeperReplId);
    }

    /** 由 replId 一致触发的 mark-up 逻辑 */
    private void markUp() {
        HEALTH_STATE preState = state.get();
        if (!preState.equals(HEALTH_STATE.INSTANCEUP)) return;
        if (state.compareAndSet(preState, HEALTH_STATE.HEALTHY)) {
            logStateChange(preState, state.get());
        }
        logger.info("[setUp] {}", this);
        notify(new InstanceUp(instance));
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
