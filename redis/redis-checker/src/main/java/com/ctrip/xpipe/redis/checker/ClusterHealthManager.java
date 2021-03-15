package com.ctrip.xpipe.redis.checker;

import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;

/**
 * @author lishanglin
 * date 2021/3/12
 */
public interface ClusterHealthManager {

    void healthCheckMasterDown(RedisHealthCheckInstance instance);

    void healthCheckMasterUp(RedisHealthCheckInstance instance);

    Observer createHealthStatusObserver();

}
