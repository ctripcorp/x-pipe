package com.ctrip.xpipe.redis.checker.healthcheck.actions.ping;

import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckActionController;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;

public interface PingActionController extends HealthCheckActionController<RedisHealthCheckInstance> {
}
