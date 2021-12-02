package com.ctrip.xpipe.redis.checker.healthcheck;

/**
 * @author chen.zhu
 * <p>
 * Sep 06, 2018
 */
public interface ActionContext<C, T extends HealthCheckInstance> {

    T instance();

    C getResult();

    long getRecvTimeMilli();

    boolean isSuccess();

    Throwable getCause();
}
