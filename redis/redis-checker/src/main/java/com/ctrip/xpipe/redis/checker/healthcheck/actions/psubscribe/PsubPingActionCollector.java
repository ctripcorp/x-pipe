package com.ctrip.xpipe.redis.checker.healthcheck.actions.psubscribe;

import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.ping.PingActionListener;

public interface PsubPingActionCollector {

    boolean supportInstance(RedisHealthCheckInstance instance);

    PingActionListener createPingActionListener();

    PsubActionListener createPsubActionListener();

}
