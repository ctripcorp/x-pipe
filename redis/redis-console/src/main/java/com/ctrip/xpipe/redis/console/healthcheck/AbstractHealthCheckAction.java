package com.ctrip.xpipe.redis.console.healthcheck;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;

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

    protected ExecutorService executors;

    protected static Random random = new Random();

    protected static int DELTA = 500;

    public AbstractHealthCheckAction(ScheduledExecutorService scheduled, RedisHealthCheckInstance instance,
                                     ExecutorService executors) {
        this.scheduled = scheduled;
        this.instance = instance;
        this.executors = executors;
    }

    @Override
    public void doStart() {
        logger.debug("[started][{}][{}]", getClass().getSimpleName(), instance.getRedisInstanceInfo());
        scheduleTask(getBaseCheckInterval());
    }

    @Override
    public void doStop() {
        if(future != null) {
            future.cancel(true);
        }
        for(HealthCheckActionListener listener : listeners) {
            listener.stopWatch(this);
        }
        listeners.clear();
        instance.unregister(this);
    }

    @Override
    public RedisHealthCheckInstance getActionInstance() {
        return instance;
    }

    @Override
    public void addListener(HealthCheckActionListener listener) {
        listeners.add(listener);
    }

    @Override
    public void removeListener(HealthCheckActionListener listener) {
        listener.stopWatch(this);
        listeners.remove(listener);
    }

    @Override
    public void addListeners(List list) {
        listeners.addAll(list);
    }

    @SuppressWarnings("unchecked")
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

    protected ScheduledFuture scheduledFuture() {
        return future;
    }

    private void scheduleTask(int baseInterval) {
        long checkInterval = getCheckTimeInterval(baseInterval);
        future = scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {

            @Override
            protected void doRun() {
                doTask();
            }
        }, checkInterval, baseInterval, TimeUnit.MILLISECONDS);
    }

    protected int getCheckTimeInterval(int baseInterval) {
        return baseInterval + (((Math.abs(random.nextInt())) % DELTA));
    }

    protected abstract void doTask();

    protected int getBaseCheckInterval() {
        return instance.getHealthCheckConfig().checkIntervalMilli();
    }


    @VisibleForTesting
    public List<HealthCheckActionListener<T>> getListeners() {
        return listeners;
    }
}
