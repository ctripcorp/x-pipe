package com.ctrip.xpipe.redis.console.healthcheck.clusteractions.beacon;

import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.beacon.BeaconServiceManager;
import com.ctrip.xpipe.redis.console.healthcheck.ClusterHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.OneWaySupport;
import com.ctrip.xpipe.redis.console.healthcheck.leader.AbstractClusterLeaderAwareHealthCheckActionFactory;
import com.ctrip.xpipe.redis.console.healthcheck.leader.SiteLeaderAwareHealthCheckAction;
import com.ctrip.xpipe.redis.console.service.meta.BeaconMetaService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;

/**
 * @author lishanglin
 * date 2021/1/15
 */
@Component
public class BeaconMetaCheckActionFactory extends AbstractClusterLeaderAwareHealthCheckActionFactory implements OneWaySupport {

    @Autowired
    private BeaconMetaService beaconMetaService;

    @Autowired
    private BeaconServiceManager beaconServiceManager;

    @Autowired
    private List<BeaconMetaController> controllers;

    @Override
    public SiteLeaderAwareHealthCheckAction create(ClusterHealthCheckInstance instance) {
        BeaconMetaCheckAction action = new BeaconMetaCheckAction(scheduled, instance, executors, beaconMetaService, beaconServiceManager);
        action.addControllers(controllers);
        return action;
    }

    @Override
    public Class<? extends SiteLeaderAwareHealthCheckAction> support() {
        return BeaconMetaCheckAction.class;
    }

    @Override
    protected List<ALERT_TYPE> alertTypes() {
        return Collections.emptyList();
    }

}
