package com.ctrip.xpipe.redis.console.healthcheck.actions.interaction;

import com.ctrip.xpipe.api.lifecycle.Startable;
import com.ctrip.xpipe.api.lifecycle.Stoppable;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.observer.AbstractObservable;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.actions.interaction.event.InstanceDown;
import com.ctrip.xpipe.redis.console.healthcheck.actions.interaction.event.InstanceHalfSick;
import com.ctrip.xpipe.redis.console.healthcheck.actions.interaction.event.InstanceSick;
import com.ctrip.xpipe.redis.console.healthcheck.actions.interaction.event.InstanceUp;
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

    private static final Logger logger = LoggerFactory.getLogger(HealthStatus.class);

    public static long UNSET_TIME = -1L;

    private AtomicLong lastPongTime = new AtomicLong(UNSET_TIME);
    private AtomicLong lastHealthDelayTime = new AtomicLong(UNSET_TIME);

    private AtomicReference<HEALTH_STATE> state = new AtomicReference<>(HEALTH_STATE.UNKNOWN);

    private RedisHealthCheckInstance instance;
    private final IntSupplier delayDownAfterMilli;
    private final IntSupplier pingDownAfterMilli;
    private final IntSupplier healthyDelayMilli;

    private final ScheduledExecutorService scheduled;
    private ScheduledFuture<?> future;

    private static Logger delayLogger = LoggerFactory.getLogger(HealthStatus.class.getName() + ".delay");

    public HealthStatus(RedisHealthCheckInstance instance, ScheduledExecutorService scheduled){
        this.instance = instance;
        this.scheduled = scheduled;
        this.pingDownAfterMilli = ()->instance.getHealthCheckConfig().pingDownAfterMilli();
        this.delayDownAfterMilli = ()->instance.getHealthCheckConfig().delayDownAfterMilli();
        this.healthyDelayMilli = ()->instance.getHealthCheckConfig().getHealthyDelayMilli();
        checkParam();
    }

    private void checkParam() {
        if(this.delayDownAfterMilli.getAsInt() < this.pingDownAfterMilli.getAsInt()) {
            logger.error("Ping-Down-After-Milli must smaller than Delay-Down-After-Milli");
        }
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

        future = scheduled.scheduleWithFixedDelay(new CheckDownTask(),
                0, instance.getHealthCheckConfig().checkIntervalMilli(), TimeUnit.MILLISECONDS);
    }

    private class CheckDownTask extends AbstractExceptionLogTask {
        @Override
        protected Logger getLogger() {
            return HealthStatus.logger;
        }

        @Override
        protected void doRun() throws Exception {

            if(lastHealthDelayTime.get() < 0 && lastPongTime.get() < 0) {
                logger.debug("[last unhealthy time < 0, break]{}, {}", instance, lastHealthDelayTime);
                return;
            }
            healthStatusUpdate();
        }
    }

    void pong(){
        lastPongTime.set(System.currentTimeMillis());
        setPingUp();
    }

    void pongInit() {
        if (lastPongTime.get() == UNSET_TIME) {
            lastPongTime.set(System.currentTimeMillis());
        }
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
        if(lastPongTime.get() != UNSET_TIME) {

            long pingDownTime = currentTime - lastPongTime.get();
            final int pingDownAfter = pingDownAfterMilli.getAsInt();
            if (pingDownTime > pingDownAfter) {
                setPingDown();
            } else if (pingDownTime >= pingDownAfter / 2) {
                setPingHalfDown();
            }
        }

        // check delay then
        if(lastHealthDelayTime.get() == UNSET_TIME) {
            return;
        }
        long delayDownTime = currentTime - lastHealthDelayTime.get();
        final int delayDownAfter = delayDownAfterMilli.getAsInt();

        if ( delayDownTime > delayDownAfter) {
            setDelayDown();
        }else if(delayDownTime >= delayDownAfter/2){
            setDelayHalfDown();
        }
    }

    private void setDelayUp() {
        HEALTH_STATE preState = state.get();
        state.compareAndSet(preState, preState.afterDelaySuccess());
        markUpIfNecessary(preState, state.get());
    }

    private void setPingUp() {
        HEALTH_STATE preState = state.get();
        state.compareAndSet(preState, preState.afterPingSuccess());
        markUpIfNecessary(preState, state.get());
    }

    private void setDelayHalfDown() {
        HEALTH_STATE preState = state.get();
        if (preState.equals(preState.afterDelayHalfFail())) {
            return;
        }
        if(state.compareAndSet(preState, preState.afterDelayHalfFail())) {
            logStateChange(preState, state.get());
            notifyObservers(new InstanceHalfSick(instance));
        }
    }

    private void setDelayDown() {
        HEALTH_STATE preState = state.get();
        if (preState.equals(preState.afterDelayFail())) {
            return;
        }
        if(state.compareAndSet(preState, preState.afterDelayFail())) {
            logStateChange(preState, state.get());
        }
        if(state.get().shouldNotifyMarkDown() && preState.isToDownNotify()){
            logger.info("[setSick]{}", this);
            notifyObservers(new InstanceSick(instance));
        }
    }

    private void setPingHalfDown() {
        HEALTH_STATE preState = state.get();
        if(preState.equals(preState.afterPingHalfFail())) {
            return;
        }
        if(state.compareAndSet(preState, preState.afterPingHalfFail())) {
            logStateChange(preState, state.get());
        }
    }

    private void setPingDown() {
        HEALTH_STATE preState = state.get();
        if(preState.equals(preState.afterPingFail())) {
            return;
        }
        if(state.compareAndSet(preState, preState.afterPingFail())) {
            logStateChange(preState, state.get());
        }
        if(state.get().shouldNotifyMarkDown() && preState.isToDownNotify()) {
            logger.info("[setDown] {}", this);
            notifyObservers(new InstanceDown(instance));
        }
    }

    private void markUpIfNecessary(HEALTH_STATE pre, HEALTH_STATE cur) {
        logStateChange(pre, cur);
        if(cur.shouldNotifyMarkup() && pre.isToUpNotify()) {
            logger.info("[markUpIfNecessary]{} {}->{}", this, pre, cur);
            notifyObservers(new InstanceUp(instance));
        }
    }

    private void logStateChange(HEALTH_STATE pre, HEALTH_STATE cur) {
        if(pre.equals(cur)) {
            return;
        }
        logger.debug("[state-change][{}] {} -> {}", this, pre, cur);
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
