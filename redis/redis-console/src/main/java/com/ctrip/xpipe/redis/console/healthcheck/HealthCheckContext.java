package com.ctrip.xpipe.redis.console.healthcheck;

import com.ctrip.xpipe.api.lifecycle.Lifecycle;
import com.ctrip.xpipe.redis.console.healthcheck.delay.DelayContext;
import com.ctrip.xpipe.redis.console.healthcheck.ping.PingContext;
import com.ctrip.xpipe.redis.console.healthcheck.redis.RedisContext;
import com.ctrip.xpipe.redis.console.healthcheck.redis.conf.RedisConfContext;

/**
 * @author chen.zhu
 * <p>
 * Aug 23, 2018
 */
public interface HealthCheckContext extends Lifecycle {

    long TIME_UNSET = -1L;

    RedisContext getRedisContext();

    DelayContext getDelayContext();

    PingContext getPingContext();

    RedisConfContext getRedisConfContext();

    HealthStatusManager.MarkDownReason lastMarkDownReason();

}
