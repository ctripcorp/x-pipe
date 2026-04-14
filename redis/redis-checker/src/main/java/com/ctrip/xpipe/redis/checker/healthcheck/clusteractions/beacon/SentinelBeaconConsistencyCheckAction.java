package com.ctrip.xpipe.redis.checker.healthcheck.clusteractions.beacon;

import com.ctrip.xpipe.redis.checker.BeaconManager;
import com.ctrip.xpipe.redis.checker.BeaconRouteType;
import com.ctrip.xpipe.redis.checker.healthcheck.CheckInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.ClusterHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckInstance;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Beacon consistency check action for sentinel mode.
 */
public class SentinelBeaconConsistencyCheckAction extends BeaconConsistencyCheckAction {

    private static final int SENTINEL_BEACON_CHECK_INTERVAL_MILLI = 15 * 1000;

    public SentinelBeaconConsistencyCheckAction(ScheduledExecutorService scheduled, ClusterHealthCheckInstance instance,
                                                ExecutorService executors, BeaconManager beaconManager) {
        super(scheduled, instance, executors, beaconManager);
    }

    @Override
    protected int getBaseCheckInterval() {
        return SENTINEL_BEACON_CHECK_INTERVAL_MILLI;
    }

    @Override
    protected BeaconRouteType getBeaconRouteType() {
        return BeaconRouteType.SENTINEL;
    }

    @Override
    protected boolean shouldCheck(HealthCheckInstance checkInstance) {
        return shouldCheckInstance(checkInstance);
    }
}
