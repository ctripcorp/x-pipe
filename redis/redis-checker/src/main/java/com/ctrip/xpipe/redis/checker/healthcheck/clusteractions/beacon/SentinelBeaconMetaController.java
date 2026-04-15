package com.ctrip.xpipe.redis.checker.healthcheck.clusteractions.beacon;

/**
 * Marker for sentinel beacon meta rules; used so Spring injects only sentinel controllers into
 * {@link SentinelBeaconConsistencyCheckActionFactory}.
 */
public interface SentinelBeaconMetaController extends BeaconMetaController {

}
