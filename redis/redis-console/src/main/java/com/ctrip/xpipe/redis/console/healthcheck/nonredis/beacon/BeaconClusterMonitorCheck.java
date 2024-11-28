package com.ctrip.xpipe.redis.console.healthcheck.nonredis.beacon;

import com.ctrip.xpipe.api.migration.auto.MonitorService;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.console.AbstractCrossDcIntervalAction;
import com.ctrip.xpipe.redis.console.migration.auto.MonitorManager;
import com.ctrip.xpipe.redis.core.beacon.BeaconSystem;
import com.ctrip.xpipe.redis.core.config.ConsoleCommonConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;

import static com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE.TOO_MANY_CLUSTERS_EXCLUDE_FROM_BEACON;

/**
 * @author lishanglin
 * date 2021/1/17
 */
@Component
public class BeaconClusterMonitorCheck extends AbstractCrossDcIntervalAction {

    @Autowired
    private MonitorManager monitorManager;

    @Autowired
    private AlertManager alertManager;

    @Autowired
    private ConsoleCommonConfig config;

    @Override
    protected List<ALERT_TYPE> alertTypes() {
        return Collections.singletonList(TOO_MANY_CLUSTERS_EXCLUDE_FROM_BEACON);
    }

    @Override
    public void doAction() {
        Map<Long, List<MonitorService>> services = monitorManager.getAllServices();
        if (services.isEmpty()) {
            logger.debug("[doCheck] no beacon service, skip");
        }

        Map<BeaconSystem, Map<Long, Set<String>>> clustersByBeaconSystemOrg = monitorManager.clustersByBeaconSystemOrg();
        if (null == clustersByBeaconSystemOrg) {
            logger.debug("[doCheck] skip for no meta");
            return;
        }

        clustersByBeaconSystemOrg.forEach(((beaconSystem, clustersByOrg) -> {
            clustersByOrg.forEach((orgId, clusters) -> {
                List<MonitorService> monitorServices = services.get(orgId);
                new UnknownClusterExcludeJob(beaconSystem, clusters, monitorServices, config.monitorUnregisterProtectCount())
                        .execute()
                        .addListener(commandFuture -> {
                            if (commandFuture.isSuccess()) {
                                logger.info("[doCheck][{}] unregister clusters {}", orgId, commandFuture.get());
                            } else if (commandFuture.cause() instanceof TooManyNeedExcludeClusterException) {
                                alertManager.alert("", "", null, ALERT_TYPE.TOO_MANY_CLUSTERS_EXCLUDE_FROM_BEACON,
                                        ((TooManyNeedExcludeClusterException) commandFuture.cause()).getNeedExcludeClusters().toString());
                            } else {
                                logger.info("[doCheck][{}] unregister clusters fail", orgId, commandFuture.cause());
                            }
                        });
            });
        }));
    }
}
