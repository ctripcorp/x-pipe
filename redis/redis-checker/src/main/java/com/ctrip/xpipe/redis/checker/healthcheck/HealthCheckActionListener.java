package com.ctrip.xpipe.redis.checker.healthcheck;

/**
 * @author chen.zhu
 * <p>
 * Sep 06, 2018
 */
public interface HealthCheckActionListener<T extends ActionContext, V extends HealthCheckAction> {

    void onAction(T t);

    boolean worksfor(ActionContext t);

    void stopWatch(V action);
}
