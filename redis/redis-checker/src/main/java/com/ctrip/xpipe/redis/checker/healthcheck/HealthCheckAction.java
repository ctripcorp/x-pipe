package com.ctrip.xpipe.redis.checker.healthcheck;

import com.ctrip.xpipe.api.lifecycle.Lifecycle;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Sep 06, 2018
 */
public interface HealthCheckAction<T extends HealthCheckInstance> extends Lifecycle {

    void addListener(HealthCheckActionListener listener);

    void removeListener(HealthCheckActionListener listener);

    void addListeners(List<HealthCheckActionListener> listeners);

    void addController(HealthCheckActionController controller);

    void addControllers(List<HealthCheckActionController> controllers);

    void removeController(HealthCheckActionController controller);

    T getActionInstance();

}
