package com.ctrip.xpipe.redis.checker.healthcheck;

/**
 * @author lishanglin
 * date 2021/1/10
 */
public interface RedisHealthCheckActionListener<T extends ActionContext> extends HealthCheckActionListener<T, HealthCheckAction<RedisHealthCheckInstance>> {
}
