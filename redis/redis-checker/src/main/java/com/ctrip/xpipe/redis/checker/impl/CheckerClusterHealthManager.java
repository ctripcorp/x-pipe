package com.ctrip.xpipe.redis.checker.impl;

import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.redis.checker.ClusterHealthManager;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author lishanglin
 * date 2021/3/12
 */
public class CheckerClusterHealthManager implements ClusterHealthManager {

    private Map<String, Set<String>> warningShards = new ConcurrentHashMap<>();

    @Override
    public void healthCheckMasterDown(RedisHealthCheckInstance instance) {

    }

    @Override
    public void healthCheckMasterUp(RedisHealthCheckInstance instance) {

    }

    @Override
    public Map<String, Set<String>> getAllWarningShards() {
        return warningShards;
    }

    @Override
    public Observer createHealthStatusObserver() {
        return null;
    }
}
