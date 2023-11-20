package com.ctrip.xpipe.redis.checker.healthcheck;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import org.slf4j.Logger;

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
public abstract class AbstractHealthCheckAction<T extends HealthCheckInstance> extends AbstractLifecycle implements HealthCheckAction<T> {

    protected List<HealthCheckActionListener> listeners = Lists.newArrayList();

    protected List<HealthCheckActionController> controllers = Lists.newArrayList();

    protected T instance;

    protected ScheduledExecutorService scheduled;

    protected ScheduledFuture future;

    protected ExecutorService executors;

    protected static Random random = new Random();

    protected static int DELTA = 500;

    public AbstractHealthCheckAction(ScheduledExecutorService scheduled, T instance,
                                     ExecutorService executors) {
        this.scheduled = scheduled;
        this.instance = instance;
        this.executors = executors;
    }

    @Override
    public void doStart() {
        logger.debug("[started][{}][{}]", getClass().getSimpleName(), instance.getCheckInfo());
        scheduleTask(getBaseCheckInterval());
    }

    @Override
    public void doStop() {
        logger.debug("[stopped][{}][{}], listener:{}, future:{}", getClass().getSimpleName(), instance.getCheckInfo(), listeners, future);
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
    public T getActionInstance() {
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

    @Override
    public void addController(HealthCheckActionController controller) {
        controllers.add(controller);
    }

    @Override
    public void addControllers(List list) {
        controllers.addAll(list);
    }

    @Override
    public void removeController(HealthCheckActionController controller) {
        controllers.remove(controller);
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

    protected void scheduleTask(int baseInterval) {
        long checkInitialDelay = getCheckInitialDelay(baseInterval);
        future = scheduled.scheduleWithFixedDelay(new ScheduledHealthCheckTask(), checkInitialDelay, baseInterval, TimeUnit.MILLISECONDS);
    }

    protected int getCheckInitialDelay(int baseInterval) {
        return baseInterval + (((Math.abs(random.nextInt())) % DELTA));
    }

    protected abstract void doTask();

    protected abstract Logger getHealthCheckLogger();

    protected boolean shouldCheck(HealthCheckInstance instance) {
        if (!instance.getCheckInfo().getClusterType().supportHealthCheck())
            return false;

        return shouldCheckInstance(instance);
    }

    protected boolean shouldCheckInstance(HealthCheckInstance instance) {
        for (HealthCheckActionController controller : controllers) {
            if (!controller.shouldCheck(instance)) {
                CheckInfo checkInfo = instance.getCheckInfo();
                if (checkInfo instanceof RedisInstanceInfo) {
                    RedisInstanceInfo redisInfo = (RedisInstanceInfo) checkInfo;
                    logger.debug("[doRun][{}+{}][{}] skip check by {}", redisInfo.getClusterId(), redisInfo.getShardId(),
                            redisInfo.getHostPort(), controller);
                } else {
                    logger.debug("[doRun][{}] skip check by {}", checkInfo.getClusterId(), controller);
                }

                return false;
            }
        }
        return true;
    }

    protected int getBaseCheckInterval() {
        return instance.getHealthCheckConfig().checkIntervalMilli();
    }

    @VisibleForTesting
    public List<HealthCheckActionListener> getListeners() {
        return listeners;
    }

    @VisibleForTesting
    public List<HealthCheckActionController> getControllers() {
        return controllers;
    }

    public class ScheduledHealthCheckTask extends AbstractExceptionLogTask {
        @Override
        protected Logger getLogger() {
            return getHealthCheckLogger();
        }

        @Override
        protected void doRun() {
            if (shouldCheck(instance)) {
                doTask();
            }
        }
    }
}
