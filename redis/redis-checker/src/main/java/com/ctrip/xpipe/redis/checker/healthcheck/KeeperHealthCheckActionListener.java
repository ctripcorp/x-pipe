package com.ctrip.xpipe.redis.checker.healthcheck;

public interface KeeperHealthCheckActionListener <T extends ActionContext> extends HealthCheckActionListener<T, HealthCheckAction<KeeperHealthCheckInstance>> {
}
