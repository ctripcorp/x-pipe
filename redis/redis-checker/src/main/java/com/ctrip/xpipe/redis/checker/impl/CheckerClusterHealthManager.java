package com.ctrip.xpipe.redis.checker.impl;

import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.redis.checker.ClusterHealthManager;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import org.springframework.stereotype.Component;

/**
 * @author lishanglin
 * date 2021/3/12
 */
@Component
public class CheckerClusterHealthManager implements ClusterHealthManager {

    @Override
    public void healthCheckMasterDown(RedisHealthCheckInstance instance) {

    }

    @Override
    public void healthCheckMasterUp(RedisHealthCheckInstance instance) {

    }

    @Override
    public Observer createHealthStatusObserver() {
        return null;
    }
}
