package com.ctrip.xpipe.redis.console.health.action;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.metric.HostPort;
import com.ctrip.xpipe.observer.AbstractObservable;
import com.ctrip.xpipe.utils.DateTimeUtils;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntSupplier;

/**
 * @author wenchao.meng
 *         <p>
 *         May 04, 2017
 */
public class HealthStatus extends AbstractObservable{

    public static final int REDIS_UNKNOWN_STATE = -1;
    public static final int REDIS_DOWN_STATE = 0;
    public static final int REDIS_UP_STATE = 1;

    private AtomicLong lastPongTime = new AtomicLong(System.currentTimeMillis());
    private AtomicLong lastHealthDelayTime = new AtomicLong(System.currentTimeMillis());

    private AtomicInteger state = new AtomicInteger(REDIS_UNKNOWN_STATE);

    private final HostPort hostPort;
    private final IntSupplier downAfterMilli;
    private final IntSupplier healthyDelayMilli;

    private final ScheduledExecutorService scheduled;
    private ScheduledFuture<?> future;

    public HealthStatus(HostPort hostPort, IntSupplier downAfterMilli, IntSupplier healthyDelayMilli, ScheduledExecutorService scheduled){
        this.hostPort = hostPort;
        this.downAfterMilli = downAfterMilli;
        this.healthyDelayMilli = healthyDelayMilli;
        this.scheduled = scheduled;
        checkDown();
    }

    private void checkDown() {

        if(future != null){
            future.cancel(true);
        }

        future = scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() throws Exception {

                long currentTime = System.currentTimeMillis();
                logger.debug("[checkDown]{} - {} = {} > {}", currentTime, lastHealthDelayTime, currentTime - lastHealthDelayTime.get(), downAfterMilli.getAsInt());
                if (currentTime - lastHealthDelayTime.get() > downAfterMilli.getAsInt()) {
                    setDown();
                }
            }
        }, 0, downAfterMilli.getAsInt()/5, TimeUnit.MILLISECONDS);
    }

    void pong(){
        lastPongTime.set(System.currentTimeMillis());
    }

    void delay(long delayMilli){

        logger.debug("[delay]{}, {}", hostPort, delayMilli);
        if(delayMilli >=0 && delayMilli <= healthyDelayMilli.getAsInt()){
            lastHealthDelayTime.set(System.currentTimeMillis());
            setUp();
        }
    }

    private void setUp() {

        int preState = state.get();
        state.set(REDIS_UP_STATE);

        if(preState != REDIS_UP_STATE){
            logger.info("[setUp]{}", this);
            notifyObservers(new InstanceUp(hostPort));
        }
    }

    private void setDown() {

        int preState = state.get();
        state.set(REDIS_DOWN_STATE);

        if(preState != REDIS_DOWN_STATE){
            logger.info("[setDown]{}", this);
            notifyObservers(new InstanceDown(hostPort));
        }
    }

    @Override
    public String toString() {
        return String.format("%s lastPong:%s lastHealthDelay:%s", hostPort,
                DateTimeUtils.timeAsString(lastPongTime.get()),
                DateTimeUtils.timeAsString(lastHealthDelayTime.get()));
    }

    public int getState() {
        return state.get();
    }
}
