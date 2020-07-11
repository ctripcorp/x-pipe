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

    void addListeners(List<HealthCheckActionListener<T>> listeners);

    void addController(HealthCheckActionController controller);

    void addControllers(List<HealthCheckActionController> controllers);

    void removeController(HealthCheckActionController controller);

    RedisHealthCheckInstance getActionInstance();

}
