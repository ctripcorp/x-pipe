package com.ctrip.xpipe.redis.console.healthcheck.nonredis.beacon;

import com.ctrip.xpipe.api.migration.auto.MonitorService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.console.AbstractCrossDcIntervalAction;
import com.ctrip.xpipe.redis.console.migration.auto.MonitorManager;
import com.ctrip.xpipe.redis.core.beacon.BeaconSystem;
import com.ctrip.xpipe.redis.core.config.ConsoleCommonConfig;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.utils.StringUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;

import static com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE.TOO_MANY_CLUSTERS_EXCLUDE_FROM_BEACON;
import static com.ctrip.xpipe.redis.console.constant.XPipeConsoleConstant.DEFAULT_ORG_ID;

/**
 * @author lishanglin
 * date 2021/1/17
 */
@Component
public class BeaconClusterMonitorCheck extends AbstractCrossDcIntervalAction {

    @Autowired
    private MonitorManager monitorManager;

    @Autowired
    private MetaCache metaCache;

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

        Map<BeaconSystem, Map<Long, Set<String>>> clustersByBeaconSystemOrg = separateClustersByBeaconSystemOrg(services.keySet());
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

    private Map<BeaconSystem, Map<Long, Set<String>>> separateClustersByBeaconSystemOrg(Set<Long> orgIds) {
        XpipeMeta xpipeMeta = metaCache.getXpipeMeta();
        if (null == xpipeMeta) return null;

        Map<BeaconSystem, Map<Long, Set<String>>> clusterByBeaconSystemOrg = new HashMap<>();
        for (BeaconSystem beaconSystem : BeaconSystem.values()) {
            Map<Long, Set<String>> clustersByOrg = new HashMap<>(orgIds.size());
            orgIds.forEach(orgId -> clustersByOrg.put(orgId, new HashSet<>()));
            clusterByBeaconSystemOrg.put(beaconSystem, clustersByOrg);
        }

        Set<String> supportZones = config.getBeaconSupportZones();
        for (DcMeta dcMeta : xpipeMeta.getDcs().values()) {
            if (!supportZones.isEmpty() && supportZones.stream().noneMatch(zone -> zone.equalsIgnoreCase(dcMeta.getZone()))) {
                logger.debug("[separateClustersByOrg][zoneUnsupported] {} not in {}", dcMeta.getId(), supportZones);
                continue;
            }

            for (ClusterMeta clusterMeta : dcMeta.getClusters().values()) {
                ClusterType clusterType = ClusterType.lookup(clusterMeta.getType());
                if (!StringUtil.isEmpty(clusterMeta.getAzGroupType())) {
                    clusterType = ClusterType.lookup(clusterMeta.getAzGroupType());
                }

                BeaconSystem beaconSystem = BeaconSystem.findByClusterType(clusterType);
                if (null == beaconSystem) {
                    continue;
                }
                if (clusterType.supportSingleActiveDC() && !dcMeta.getId().equalsIgnoreCase(clusterMeta.getActiveDc())) {
                    // only register cluster whose active dc is in supported zone
                    continue;
                }

                Map<Long, Set<String>> clustersByOrg = clusterByBeaconSystemOrg.get(beaconSystem);
                if (orgIds.contains((long) clusterMeta.getOrgId())) {
                    clustersByOrg.get((long) clusterMeta.getOrgId()).add(clusterMeta.getId());
                } else if (orgIds.contains(DEFAULT_ORG_ID)) {
                    clustersByOrg.get(DEFAULT_ORG_ID).add(clusterMeta.getId());
                }
            }
        }

        return clusterByBeaconSystemOrg;
    }

}
