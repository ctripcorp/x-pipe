package com.ctrip.xpipe.redis.console.healthcheck.sentinel;

import com.ctrip.xpipe.redis.console.healthcheck.ActionContext;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckActionListener;

/**
 * @author chen.zhu
 * <p>
 * Oct 09, 2018
 */
public class SentinelHelloActionListener implements HealthCheckActionListener<SentinelActionContext> {

    private SentinelHelloCollector collector;

    public SentinelHelloActionListener(SentinelHelloCollector collector) {
        this.collector = collector;
    }

    @Override
    public void onAction(SentinelActionContext context) {
        collector.collect(context);
    }

    @Override
    public boolean worksfor(ActionContext t) {
        return t instanceof SentinelActionContext;
    }

    @Override
    public void stopWatch(HealthCheckAction action) {}
}
