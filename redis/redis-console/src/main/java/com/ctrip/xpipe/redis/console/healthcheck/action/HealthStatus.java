package com.ctrip.xpipe.redis.console.healthcheck.action;

import com.ctrip.xpipe.api.lifecycle.Startable;
import com.ctrip.xpipe.api.lifecycle.Stoppable;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.observer.AbstractObservable;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.action.event.InstanceDown;
import com.ctrip.xpipe.redis.console.healthcheck.action.event.InstanceSick;
import com.ctrip.xpipe.redis.console.healthcheck.action.event.InstanceUp;
import com.ctrip.xpipe.utils.DateTimeUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntSupplier;

/**
 * @author wenchao.meng
 *         <p>
 *         May 04, 2017
 */
public class HealthStatus extends AbstractObservable implements Startable, Stoppable {

    public static long UNSET_TIME = -1L;

    public static int PING_DOWN_AFTER_MILLI = 30 * 1000;

    private AtomicLong lastPongTime = new AtomicLong(UNSET_TIME);
    private AtomicLong lastHealthDelayTime = new AtomicLong(UNSET_TIME);

    private AtomicReference<HEALTH_STATE> state = new AtomicReference<>(HEALTH_STATE.UNKNOWN);

    private RedisHealthCheckInstance instance;
    private final IntSupplier downAfterMilli;
    private final IntSupplier healthyDelayMilli;

    private final ScheduledExecutorService scheduled;
    private ScheduledFuture<?> future;

    private static Logger delayLogger = LoggerFactory.getLogger(HealthStatus.class.getName() + ".delay");

    public HealthStatus(RedisHealthCheckInstance instance, ScheduledExecutorService scheduled){
        this.instance = instance;
        this.downAfterMilli = ()->instance.getHealthCheckConfig().downAfterMilli();
        this.healthyDelayMilli = ()->instance.getHealthCheckConfig().getHealthyDelayMilli();
        this.scheduled = scheduled;
    }

    @Override
    public void start() {
        checkDown();
    }

    @Override
    public void stop() {
        if(future != null) {
            future.cancel(true);
        }
    }

    private void checkDown() {

        if(future != null){
            future.cancel(true);
        }

        future = scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() throws Exception {
                if(lastHealthDelayTime.get() < 0 || lastPongTime.get() < 0){
                    logger.debug("[last unhealthy time < 0, break]{}, {}", instance, lastHealthDelayTime);
                    return;
                }
                healthStatusUpdate();
            }
        }, 0, PING_DOWN_AFTER_MILLI / 10, TimeUnit.MILLISECONDS);
    }

    void pong(){
        lastPongTime.set(System.currentTimeMillis());
        setPingUp();
    }

    void delay(long delayMilli){

        //first time
        lastHealthDelayTime.compareAndSet(UNSET_TIME, System.currentTimeMillis());

        delayLogger.debug("{}, {}", instance.getRedisInstanceInfo().getHostPort(), delayMilli);
        if(delayMilli >= 0 && delayMilli <= healthyDelayMilli.getAsInt()){
            lastHealthDelayTime.set(System.currentTimeMillis());
            setDelayUp();
        }
    }

    @VisibleForTesting
    protected void healthStatusUpdate() {
        long currentTime = System.currentTimeMillis();

        // check ping down first, as ping has highest priority
        long pingDownTime = currentTime - lastPongTime.get();
        if(pingDownTime >= PING_DOWN_AFTER_MILLI) {
            setPingDown();
        }

        // check delay then
        long delayDownTime = currentTime - lastHealthDelayTime.get();
        final int  downAfter = downAfterMilli.getAsInt();

        if ( delayDownTime > downAfter) {
            setDelayDown();
        }else if(delayDownTime >= downAfter/2){
            setDelayHalfDown();
        }
    }

    private void setDelayUp() {
        HEALTH_STATE preState = state.get();
        state.set(preState.afterDelaySuccess());
        markUpIfNecessary(preState, state.get());
    }

    private void setPingUp() {
        HEALTH_STATE preState = state.get();
        state.set(preState.afterPingSuccess());
        markUpIfNecessary(preState, state.get());
    }

    private void setDelayHalfDown() {

        HEALTH_STATE preState = state.get();
        state.set(preState.afterDelayHalfFail());
        if(preState != HEALTH_STATE.UNHEALTHY && state.get() == HEALTH_STATE.UNHEALTHY){
            logger.info("[setDelayHalfDown]{}, {}", this, preState);
        }
    }

    private void setDelayDown() {

        HEALTH_STATE preState = state.get();
        state.set(preState.afterDelayFail());

        if(state.get().markDown() && preState.isToDownNotify()){
            logger.info("[setDelayDown]{}", this);
            notifyObservers(new InstanceSick(instance));
        }
    }

    private void setPingDown() {
        HEALTH_STATE preState = state.get();
        state.set(preState.afterPingFail());
        if(state.get().markDown() && preState.isToDownNotify()) {
            logger.info("[setPingDown] {}", this);
            notifyObservers(new InstanceDown(instance));
        }
    }

    private void markUpIfNecessary(HEALTH_STATE pre, HEALTH_STATE cur) {
        if(cur.markUp() && pre.isToUpNotify()) {
            logger.info("[markUp]{}", this);
            notifyObservers(new InstanceUp(instance));
        }
    }

    @Override
    public String toString() {
        return String.format("%s lastPong:%s lastHealthDelay:%s", instance.getRedisInstanceInfo(),
                DateTimeUtils.timeAsString(lastPongTime.get()),
                DateTimeUtils.timeAsString(lastHealthDelayTime.get()));
    }

    public HEALTH_STATE getState() {
        return state.get();
    }
}
