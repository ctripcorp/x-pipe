package com.ctrip.xpipe.redis.checker.healthcheck.clusteractions.beacon;

import com.ctrip.xpipe.redis.checker.BeaconManager;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.healthcheck.BiDirectionSupport;
import com.ctrip.xpipe.redis.checker.healthcheck.ClusterHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.OneWaySupport;
import com.ctrip.xpipe.redis.checker.healthcheck.leader.AbstractClusterLeaderAwareHealthCheckActionFactory;
import com.ctrip.xpipe.redis.checker.healthcheck.leader.SiteLeaderAwareHealthCheckAction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;

/**
 * @author lishanglin
 * date 2021/1/15
 */
@Component
public class BeaconMetaCheckActionFactory extends AbstractClusterLeaderAwareHealthCheckActionFactory implements OneWaySupport, BiDirectionSupport {

    @Autowired
    private BeaconManager beaconManager;

    @Autowired
    private List<BeaconMetaController> controllers;

    @Override
    public SiteLeaderAwareHealthCheckAction create(ClusterHealthCheckInstance instance) {
        BeaconMetaCheckAction action = new BeaconMetaCheckAction(scheduled, instance, executors, beaconManager);
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
