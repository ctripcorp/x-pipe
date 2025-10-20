package com.ctrip.xpipe.redis.checker.healthcheck.leader;

import com.ctrip.xpipe.redis.checker.healthcheck.ClusterHealthCheckActionFactory;
import com.ctrip.xpipe.redis.checker.healthcheck.ClusterHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckInstanceManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;

import java.util.List;

/**
 * @author lishanglin
 * date 2021/1/15
 */
public abstract class AbstractClusterLeaderAwareHealthCheckActionFactory extends AbstractLeaderAwareHealthCheckActionFactory<ClusterHealthCheckInstance>
        implements ClusterHealthCheckActionFactory<SiteLeaderAwareHealthCheckAction> {

    @Autowired
    @Lazy
    protected HealthCheckInstanceManager healthCheckInstanceManager;

    @Override
    protected List<ClusterHealthCheckInstance> getAllInstances() {
        return healthCheckInstanceManager.getAllClusterInstance();
    }

}
