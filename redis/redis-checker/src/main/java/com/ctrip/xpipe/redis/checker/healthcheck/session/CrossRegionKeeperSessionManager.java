package com.ctrip.xpipe.redis.checker.healthcheck.session;

/**
 * Session owner for the Keeper endpoints an {@code InfoReplIdAction} reads repl-id from.
 *
 * The name refers to the <em>cluster</em> crossing regions, not to the endpoint: the Keeper is
 * local to the checker. It is looked up under the redis' own dc, so it sits in this dc's cluster
 * entry in the meta -- the reason it still needs its own manager is the cluster's {@code activeDc},
 * see {@link DefaultCrossRegionKeeperSessionManager}.
 *
 * Not extending {@link DefaultKeeperSessionManager} on purpose: it keeps this bean out of
 * {@code KeeperSessionManager} injection points, so the existing {@code @Primary}-free wiring of
 * {@link DefaultKeeperSessionManager} stays intact, and it inherits
 * {@link AbstractInstanceSessionManager#findOrCreateSession(com.ctrip.xpipe.endpoint.HostPort)},
 * which resolves endpoints through {@code HealthCheckEndpointFactory} -- the same resolution the
 * paired redis instance uses -- instead of the direct {@code DefaultEndPoint} that
 * {@link DefaultKeeperSessionManager} builds for its same-dc Keepers.
 */
public interface CrossRegionKeeperSessionManager extends InstanceSessionManager {

}
