package com.ctrip.xpipe.redis.checker.healthcheck;

/**
 * @author lishanglin
 * date 2021/1/15
 */
public interface ClusterHealthCheckActionFactory<T extends HealthCheckAction> extends HealthCheckActionFactory<T, ClusterHealthCheckInstance> {
}
