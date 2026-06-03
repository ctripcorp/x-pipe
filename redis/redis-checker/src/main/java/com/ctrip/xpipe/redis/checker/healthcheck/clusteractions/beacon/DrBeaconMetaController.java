package com.ctrip.xpipe.redis.checker.healthcheck.clusteractions.beacon;

/**
 * Marker for DR beacon meta rules; used so Spring injects only DR controllers into
 * {@link BeaconConsistencyCheckActionFactory}.
 */
public interface DrBeaconMetaController extends BeaconMetaController {

}
