package com.ctrip.xpipe.redis.checker.healthcheck;

public interface HealthCheckActionController<T extends HealthCheckInstance> {

    boolean shouldCheck(T instance);

}
