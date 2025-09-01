package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction;

import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.delay.DelayActionListener;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.ping.PingActionListener;

/**
 * @author chen.zhu
 * <p>
 * Oct 11, 2018
 */
public interface DelayPingActionCollector {

    boolean supportInstance(RedisHealthCheckInstance instance);

    PingActionListener createPingActionListener(RedisHealthCheckInstance instance);

    DelayActionListener createDelayActionListener(RedisHealthCheckInstance instance);

}
