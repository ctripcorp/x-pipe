package com.ctrip.xpipe.redis.console.healthcheck;

public interface HealthCheckActionController<T extends HealthCheckInstance> {

    boolean shouldCheck(T instance);

}
