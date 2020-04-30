package com.ctrip.xpipe.redis.console.healthcheck;

public interface HealthCheckActionController {

    boolean shouldCheck(RedisHealthCheckInstance instance);

}
