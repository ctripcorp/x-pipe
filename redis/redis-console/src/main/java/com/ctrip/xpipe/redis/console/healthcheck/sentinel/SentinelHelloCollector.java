package com.ctrip.xpipe.redis.console.healthcheck.sentinel;

import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckActionListener;

/**
 * @author chen.zhu
 * <p>
 * Oct 09, 2018
 */
public interface SentinelHelloCollector {

    HealthCheckActionListener<SentinelActionContext> getSentinelHelloActionListener();

    void collect(SentinelActionContext context);
}
