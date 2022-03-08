package com.ctrip.xpipe.redis.checker.healthcheck.leader;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.checker.healthcheck.AbstractHealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckInstance;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author chen.zhu
 * <p>
 * Oct 06, 2018
 */
public abstract class AbstractLeaderAwareHealthCheckAction<T extends HealthCheckInstance> extends AbstractHealthCheckAction<T> implements SiteLeaderAwareHealthCheckAction<T> {

    private static final int START_TIME_INTERVAL_MILLI = 5 * 60 * 1000;

    public AbstractLeaderAwareHealthCheckAction(ScheduledExecutorService scheduled, T instance, ExecutorService executors) {
        super(scheduled, instance, executors);
    }

    @Override
    protected int getBaseCheckInterval() {
        return getActionInstance().getHealthCheckConfig().getRedisConfCheckIntervalMilli();
    }

    @Override
    protected int getCheckInitialDelay(int baseInterval) {
        return Math.abs(random.nextInt(START_TIME_INTERVAL_MILLI) % START_TIME_INTERVAL_MILLI);
    }


    protected abstract class AsyncRun {

        public void run() {
            executors.execute(new AbstractExceptionLogTask() {
                @Override
                protected void doRun() throws Exception {
                    doRun0();
                }
            });
        }

        protected abstract void doRun0();
    }
}
