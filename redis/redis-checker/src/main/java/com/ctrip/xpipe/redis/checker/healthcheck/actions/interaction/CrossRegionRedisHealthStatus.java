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
    private volatile String keeperReplId2;

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

    /** InfoReplIdAction 的 listener 写回；replId 或 replId2 一致时拉入 */
    public void updateReplIds(String slaveReplId, String keeperReplId, String keeperReplId2) {
        this.slaveReplId = slaveReplId;
        this.keeperReplId = keeperReplId;
        this.keeperReplId2 = keeperReplId2;
        boolean match = replIdMatch();
        if(!match) {
            logger.info("[updateReplIds] {} slaveReplId={}, keeperReplId={}, keeperReplId2={}, match={}, state={}",
                    instance.getCheckInfo().getHostPort(), slaveReplId, keeperReplId, keeperReplId2, match, state.get());
        }
        if (match) {
            markUp();
        }
    }

    /** 校验逻辑：slave 的 master_replid 匹配 keeper 的 master_replid 或 master_replid2（或关系） */
    public boolean replIdMatch() {
        return slaveReplId != null
                && ((keeperReplId != null && slaveReplId.equals(keeperReplId))
                    || (keeperReplId2 != null && slaveReplId.equals(keeperReplId2)));
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
