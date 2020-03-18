package com.ctrip.xpipe.redis.console.healthcheck;

import com.ctrip.xpipe.api.lifecycle.Lifecycle;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Sep 06, 2018
 */
public interface HealthCheckAction<T extends ActionContext> extends Lifecycle {

    void addListener(HealthCheckActionListener<T> listener);

    void removeListener(HealthCheckActionListener<T> listener);

    default void addController(HealthCheckActionController controller) {}

    default void removeController(HealthCheckActionController controller) {}

    void addListeners(List<HealthCheckActionListener<T>> listeners);

    RedisHealthCheckInstance getActionInstance();

}
