package com.ctrip.xpipe.redis.console.healthcheck;

import com.ctrip.xpipe.api.lifecycle.Lifecycle;
import com.ctrip.xpipe.redis.console.healthcheck.config.HealthCheckConfig;

import java.util.List;

/**
 * @author lishanglin
 * date 2021/1/10
 */
public interface HealthCheckInstance<T extends CheckInfo> extends Lifecycle {

    T getCheckInfo();

    HealthCheckConfig getHealthCheckConfig();

    void register(HealthCheckAction action);

    void unregister(HealthCheckAction action);

    List<HealthCheckAction> getHealthCheckActions();

}
