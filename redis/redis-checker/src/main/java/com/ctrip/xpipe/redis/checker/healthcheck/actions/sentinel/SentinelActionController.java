package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel;

import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckActionController;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;

public interface SentinelActionController extends HealthCheckActionController<RedisHealthCheckInstance> {
}
