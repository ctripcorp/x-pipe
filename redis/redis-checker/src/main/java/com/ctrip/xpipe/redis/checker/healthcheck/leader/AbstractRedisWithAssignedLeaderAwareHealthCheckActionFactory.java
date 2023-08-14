package com.ctrip.xpipe.redis.checker.healthcheck.leader;

import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckInstanceManager;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckActionFactory;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

/**
 * @author lishanglin
 * date 2021/1/15
 */
public abstract class AbstractRedisWithAssignedLeaderAwareHealthCheckActionFactory extends AbstractLeaderAwareHealthCheckActionFactory<RedisHealthCheckInstance>
        implements RedisHealthCheckActionFactory<SiteLeaderAwareHealthCheckAction> {

    @Autowired
    private HealthCheckInstanceManager healthCheckInstanceManager;

    @Override
    protected List<RedisHealthCheckInstance> getAllInstances() {
        return healthCheckInstanceManager.getAllRedisInstanceForAssignedAction();
    }

}
