package com.ctrip.xpipe.redis.integratedtest.health;


import com.ctrip.xpipe.api.lifecycle.Releasable;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.console.health.delay.MetricDelayCollector;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

/**
 * @author chen.zhu
 * <p>
 * Jul 29, 2018
 */
public class NaiveHealthChecker implements HealthChecker {

    private List<Releasable> resources = Lists.newArrayList();

    private ScheduledFuture future;

    private ScheduledExecutorService scheduled;

    private MetricDelayCollector collector = new MetricDelayCollector();

    @Override
    public void release() throws Exception {
        for(Releasable resource : resources) {
            resource.release();
        }
    }

    @Override
    public void start() throws Exception {

    }

    @Override
    public void stop() throws Exception {

    }

    private void scheduleHealthCheck() {

    }

    public class DelayHealthCheckTask extends AbstractExceptionLogTask {

        @Override
        protected void doRun() throws Exception {

        }
    }
}
