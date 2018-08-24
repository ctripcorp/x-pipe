package com.ctrip.xpipe.redis.console.healthcheck.action;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import org.springframework.stereotype.Component;

/**
 * @author wenchao.meng
 *         <p>
 *         May 05, 2017
 */
@Component
public class CatHealthEventProcessor implements DelayHealthEventProcessor, PingHealthEventProcessor {

    private static final String TYPE = "HealthEvent";

    @Override
    public void markDown(RedisHealthCheckInstance instance) {
        EventMonitor.DEFAULT.logEvent(TYPE, String.format("InstanceDown-%s", instance.getRedisInstanceInfo().getHostPort()));
    }

    @Override
    public void markUp(RedisHealthCheckInstance instance) {
        EventMonitor.DEFAULT.logEvent(TYPE, String.format("InstanceUp-%s", instance.getRedisInstanceInfo().getHostPort()));
    }
}
