package com.ctrip.xpipe.redis.checker.healthcheck;

public interface KeeperHealthCheckActionFactory <T extends HealthCheckAction>  extends HealthCheckActionFactory<T, KeeperHealthCheckInstance>{
}
