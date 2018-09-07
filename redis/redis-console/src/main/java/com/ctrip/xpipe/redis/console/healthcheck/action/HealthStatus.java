package com.ctrip.xpipe.redis.console.healthcheck.action;

import com.ctrip.xpipe.api.lifecycle.Startable;
import com.ctrip.xpipe.api.lifecycle.Stoppable;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.observer.AbstractObservable;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.action.HEALTH_STATE;
import com.ctrip.xpipe.utils.DateTimeUtils;
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

                if(lastHealthDelayTime.get() < 0){
                    logger.debug("[last unhealthy time < 0, break]{}, {}", instance, lastHealthDelayTime);
                    return;
                }

                long currentTime = System.currentTimeMillis();
                logger.trace("[checkDown]{} - {} = {} > {}", currentTime, lastHealthDelayTime, currentTime - lastHealthDelayTime.get(), downAfterMilli.getAsInt());
                long downTime = currentTime - lastHealthDelayTime.get();
                final int  downAfter = downAfterMilli.getAsInt();

                if ( downTime > downAfter) {
                    setDown();
                }else if(downTime >= downAfter/2){
                    setUnhealthy();
                }

            }
        }, 0, downAfterMilli.getAsInt()/5, TimeUnit.MILLISECONDS);
    }

    void pong(){
        lastPongTime.set(System.currentTimeMillis());
    }

    void delay(long delayMilli){

        //first time
        lastHealthDelayTime.compareAndSet(UNSET_TIME, System.currentTimeMillis());

        delayLogger.debug("{}, {}", instance.getRedisInstanceInfo().getHostPort(), delayMilli);
        if(delayMilli >= 0 && delayMilli <= healthyDelayMilli.getAsInt()){
            lastHealthDelayTime.set(System.currentTimeMillis());
            setUp();
        }
    }

    private void setUp() {

        HEALTH_STATE preState = state.get();
        state.set(HEALTH_STATE.UP);

        if(preState.isToUpNotify()){
            logger.info("[setUp]{}", this);
            notifyObservers(new InstanceUp(instance));
        }
    }

    private void setUnhealthy() {

        HEALTH_STATE previous = state.getAndSet(HEALTH_STATE.UNHEALTHY);
        if(previous != HEALTH_STATE.UNHEALTHY){
            logger.info("[setUnhealthy]{}, {}", this, previous);
        }
    }

    private void setDown() {

        HEALTH_STATE preState = state.get();
        state.set(HEALTH_STATE.DOWN);

        if(preState.isToDownNotify()){
            logger.info("[setDown]{}", this);
            notifyObservers(new InstanceDown(instance));
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
