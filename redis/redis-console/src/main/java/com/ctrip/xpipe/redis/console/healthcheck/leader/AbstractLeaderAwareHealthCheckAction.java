package com.ctrip.xpipe.redis.console.healthcheck.leader;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.console.healthcheck.AbstractHealthCheckAction;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author chen.zhu
 * <p>
 * Oct 06, 2018
 */
public abstract class AbstractLeaderAwareHealthCheckAction extends AbstractHealthCheckAction implements SiteLeaderAwareHealthCheckAction {

    private static final int START_TIME_INTERVAL_MILLI = 5 * 60 * 1000;

    public AbstractLeaderAwareHealthCheckAction(ScheduledExecutorService scheduled, RedisHealthCheckInstance instance, ExecutorService executors) {
        super(scheduled, instance, executors);
    }

    @Override
    protected int getBaseCheckInterval() {
        return getActionInstance().getHealthCheckConfig().getRedisConfCheckIntervalMilli();
    }

    @Override
    protected int getCheckTimeInterval(int baseInterval) {
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
