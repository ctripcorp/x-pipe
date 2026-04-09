package com.ctrip.xpipe.redis.console.healthcheck.nonredis.beacon;

import com.ctrip.xpipe.api.migration.auto.MonitorService;
import com.ctrip.xpipe.redis.checker.BeaconRouteType;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.console.AbstractSiteLeaderIntervalAction;
import com.ctrip.xpipe.redis.console.migration.auto.MonitorManager;
import com.ctrip.xpipe.redis.core.beacon.BeaconSystem;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Remove redundant clusters from sentinel beacon route.
 */
@Component
public class SentinelBeaconClusterMonitorCheck extends AbstractSiteLeaderIntervalAction {

    @Autowired
    private MonitorManager monitorManager;

    @Autowired
    private AlertManager alertManager;

    @Override
    protected List<ALERT_TYPE> alertTypes() {
        return Collections.singletonList(ALERT_TYPE.TOO_MANY_CLUSTERS_EXCLUDE_FROM_BEACON);
    }

    @Override
    protected void doAction() {
        Map<Long, List<MonitorService>> services = monitorManager.getAllServices(BeaconRouteType.SENTINEL);
        if (services.isEmpty()) {
            logger.debug("[doCheck][sentinel beacon] no beacon service, skip");
            return;
        }

        Map<BeaconSystem, Map<Long, Set<String>>> clustersByBeaconSystemOrg =
                monitorManager.clustersByBeaconSystemOrg(BeaconRouteType.SENTINEL);
        if (clustersByBeaconSystemOrg == null) {
            logger.debug("[doCheck][sentinel beacon] skip for no meta");
            return;
        }

        clustersByBeaconSystemOrg.forEach((beaconSystem, clustersByOrg) -> clustersByOrg.forEach((orgId, clusters) -> {
            List<MonitorService> monitorServices = services.get(orgId);
            if (monitorServices == null || monitorServices.isEmpty()) {
                return;
            }
            // For sentinel mode cleanup, disable protection to avoid stale registrations after large DR switches.
            new UnknownClusterExcludeJob(beaconSystem, clusters, monitorServices, Integer.MAX_VALUE)
                    .execute()
                    .addListener(commandFuture -> {
                        if (commandFuture.isSuccess()) {
                            logger.info("[doCheck][sentinel beacon][{}] unregister clusters {}", orgId, commandFuture.get());
                        } else if (commandFuture.cause() instanceof TooManyNeedExcludeClusterException) {
                            alertManager.alert("", "", null, ALERT_TYPE.TOO_MANY_CLUSTERS_EXCLUDE_FROM_BEACON,
                                    ((TooManyNeedExcludeClusterException) commandFuture.cause()).getNeedExcludeClusters().toString());
                        } else {
                            logger.info("[doCheck][sentinel beacon][{}] unregister clusters fail", orgId, commandFuture.cause());
                        }
                    });
        }));
    }
}
