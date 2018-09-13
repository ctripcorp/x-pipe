package com.ctrip.xpipe.redis.console.healthcheck;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.google.common.collect.Lists;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author chen.zhu
 * <p>
 * Sep 06, 2018
 */
public abstract class AbstractHealthCheckAction<T extends ActionContext> extends AbstractLifecycle implements HealthCheckAction {

    protected List<HealthCheckActionListener<T>> listeners = Lists.newArrayList();

    protected RedisHealthCheckInstance instance;

    protected ScheduledExecutorService scheduled;

    private ScheduledFuture future;

    private ExecutorService executors;

    private static Random random = new Random();

    protected static int DELTA = 500;

    public AbstractHealthCheckAction(ScheduledExecutorService scheduled, RedisHealthCheckInstance instance,
                                     ExecutorService executors) {
        this.scheduled = scheduled;
        this.instance = instance;
        this.executors = executors;
        instance.register(this);
    }

    @Override
    protected void doInitialize() throws Exception {
        super.doInitialize();
    }

    @Override
    public void doStart() throws Exception {
        super.doStart();
        scheduleTask(getBaseCheckInterval());
    }

    @Override
    public void doStop() throws Exception {
        if(future != null) {
            future.cancel(true);
        }
        for(HealthCheckActionListener listener : listeners) {
            removeListener(listener);
        }
        instance.unregister(this);
        super.doStop();
    }

    @Override
    public void addListener(HealthCheckActionListener listener) {
        listeners.add(listener);
    }

    @Override
    public void removeListener(HealthCheckActionListener listener) {
        listeners.remove(listener);

    }

    @Override
    public void addListeners(List list) {
        listeners.addAll(list);
    }

    protected void notifyListeners(ActionContext context) {
        for(HealthCheckActionListener listener : listeners) {
            if(listener.worksfor(context)) {
                executors.execute(new AbstractExceptionLogTask() {
                    @Override
                    protected void doRun() throws Exception {
                        listener.onAction(context);
                    }
                });
            }
        }
    }

    protected void scheduleTask(int baseInterval) {
        long checkInterval = getCheckTimeInterval(baseInterval);
        future = scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {

            @Override
            protected void doRun() {
                doScheduledTask();
            }
        }, checkInterval, baseInterval, TimeUnit.MILLISECONDS);
    }

    private int getCheckTimeInterval(int baseInterval) {
        return baseInterval + (((Math.abs(random.nextInt())) % DELTA));
    }

    protected int getWarmupTime() {
        int base = Math.min(getBaseCheckInterval(), 1000);
        int result = (Math.abs(random.nextInt()) % base);
        return result == 0 ? DELTA : (result == base ? result - DELTA : result);
    }

    protected abstract void doScheduledTask();

    protected int getBaseCheckInterval() {
        return instance.getHealthCheckConfig().checkIntervalMilli();
    }


}
