package com.ctrip.xpipe.redis.checker.healthcheck.leader;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckInstanceManager;
import com.ctrip.xpipe.redis.checker.healthcheck.KeeperHealthCheckActionFactory;
import com.ctrip.xpipe.redis.checker.healthcheck.KeeperHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.KeeperSupport;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

public abstract class AbstractKeeperLeaderAwareHealthCheckActionFactory
        extends AbstractLeaderAwareHealthCheckActionFactory<KeeperHealthCheckInstance>
        implements KeeperHealthCheckActionFactory<SiteLeaderAwareHealthCheckAction> {

    @Autowired
    private HealthCheckInstanceManager healthCheckInstanceManager;

    @Override
    protected List<KeeperHealthCheckInstance> getAllInstances() {
        return healthCheckInstanceManager.getAllKeeperInstance();
    }

    @Override
    protected void registerInstance(KeeperHealthCheckInstance instance) {
         ClusterType clusterType = instance.getCheckInfo().getClusterType();

         if ((clusterType.equals(ClusterType.ONE_WAY) || clusterType.equals(ClusterType.HETERO))
                 && AbstractKeeperLeaderAwareHealthCheckActionFactory.this instanceof KeeperSupport){
             registerTo(instance);
         }
    }
}
