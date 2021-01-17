package com.ctrip.xpipe.redis.console.healthcheck.leader;

import com.ctrip.xpipe.redis.console.healthcheck.ClusterHealthCheckActionFactory;
import com.ctrip.xpipe.redis.console.healthcheck.ClusterHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckInstanceManager;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

/**
 * @author lishanglin
 * date 2021/1/15
 */
public abstract class AbstractClusterLeaderAwareHealthCheckActionFactory extends AbstractLeaderAwareHealthCheckActionFactory<ClusterHealthCheckInstance>
        implements ClusterHealthCheckActionFactory<SiteLeaderAwareHealthCheckAction> {

    @Autowired
    private HealthCheckInstanceManager healthCheckInstanceManager;

    @Override
    protected List<ClusterHealthCheckInstance> getAllInstances() {
        return healthCheckInstanceManager.getAllClusterInstance();
    }

}
