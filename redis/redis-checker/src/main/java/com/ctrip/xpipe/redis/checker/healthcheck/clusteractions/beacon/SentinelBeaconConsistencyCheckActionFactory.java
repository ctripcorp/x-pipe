package com.ctrip.xpipe.redis.checker.healthcheck.clusteractions.beacon;

import com.ctrip.xpipe.redis.checker.BeaconManager;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.healthcheck.ClusterHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.LocalDcSupport;
import com.ctrip.xpipe.redis.checker.healthcheck.OneWaySupport;
import com.ctrip.xpipe.redis.checker.healthcheck.SingleDcSupport;
import com.ctrip.xpipe.redis.checker.healthcheck.leader.AbstractClusterLeaderAwareHealthCheckActionFactory;
import com.ctrip.xpipe.redis.checker.healthcheck.leader.SiteLeaderAwareHealthCheckAction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;

/**
 * Checker action factory for sentinel beacon mode.
 */
@Component
public class SentinelBeaconConsistencyCheckActionFactory extends AbstractClusterLeaderAwareHealthCheckActionFactory
        implements OneWaySupport, SingleDcSupport, LocalDcSupport {

    @Autowired
    private BeaconManager beaconManager;

    @Autowired
    private List<SentinelBeaconMetaController> controllers;

    @Override
    public SiteLeaderAwareHealthCheckAction create(ClusterHealthCheckInstance instance) {
        SentinelBeaconConsistencyCheckAction action = new SentinelBeaconConsistencyCheckAction(scheduled, instance, executors, beaconManager);
        action.addControllers(controllers);
        return action;
    }

    @Override
    public Class<? extends SiteLeaderAwareHealthCheckAction> support() {
        return SentinelBeaconConsistencyCheckAction.class;
    }

    @Override
    protected List<ALERT_TYPE> alertTypes() {
        return Collections.emptyList();
    }

    @Override
    public boolean supportInstnace(ClusterHealthCheckInstance instance) {
        return true;
    }
}
