package com.ctrip.xpipe.redis.console.healthcheck.nonredis.beacon;

import com.ctrip.xpipe.api.migration.auto.MonitorService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.AbstractCrossDcIntervalCheck;
import com.ctrip.xpipe.redis.console.migration.auto.MonitorServiceManager;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
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
public class BeaconClusterMonitorCheck extends AbstractCrossDcIntervalCheck {

    @Autowired
    private MonitorServiceManager monitorServiceManager;

    @Autowired
    private MetaCache metaCache;

    @Autowired
    private AlertManager alertManager;

    @Autowired
    private ConsoleConfig config;

    @Override
    protected List<ALERT_TYPE> alertTypes() {
        return Collections.singletonList(TOO_MANY_CLUSTERS_EXCLUDE_FROM_BEACON);
    }

    @Override
    public void doCheck() {
        Map<Long, MonitorService> services = monitorServiceManager.getAllServices();
        if (services.isEmpty()) {
            logger.debug("[doCheck] no beacon service, skip");
        }

        Map<Long, Set<String>> clustersByOrg = separateClustersByOrg(services.keySet());
        if (null == clustersByOrg) {
            logger.debug("[doCheck] skip for no meta");
            return;
        }

        clustersByOrg.forEach((orgId, clusters) -> {
            new UnknownClusterExcludeJob(clusters, services.get(orgId), config.monitorUnregisterProtectCount()).execute()
                .addListener(commandFuture -> {
                    if (commandFuture.isSuccess()) {
                        logger.info("[doCheck][{}] unregister clusters {}", orgId, commandFuture.get());
                    } else if (commandFuture.cause() instanceof TooManyNeedExcludeClusterException) {
                        alertManager.alert("", "", null, ALERT_TYPE.TOO_MANY_CLUSTERS_EXCLUDE_FROM_BEACON,
                                ((TooManyNeedExcludeClusterException)commandFuture.cause()).getNeedExcludeClusters().toString());
                    } else {
                        logger.info("[doCheck][{}] unregister clusters fail", orgId, commandFuture.cause());
                    }
                });
        });
    }

    private Map<Long, Set<String>> separateClustersByOrg(Set<Long> orgIds) {
        XpipeMeta xpipeMeta = metaCache.getXpipeMeta();
        if (null == xpipeMeta) return null;

        Map<Long, Set<String>> clustersByOrg = new HashMap<>(orgIds.size());
        orgIds.forEach(orgId -> clustersByOrg.put(orgId, new HashSet<>()));

        for (DcMeta dcMeta : xpipeMeta.getDcs().values()) {
            for (ClusterMeta clusterMeta : dcMeta.getClusters().values()) {
                if (!ClusterType.lookup(clusterMeta.getType()).supportMigration()) {
                    continue;
                }

                if (config.getMigrationUnsupportedClusters().contains(clusterMeta.getId().toLowerCase())) {
                    continue;
                }

                if (orgIds.contains((long)clusterMeta.getOrgId())) {
                    clustersByOrg.get((long)clusterMeta.getOrgId()).add(clusterMeta.getId());
                } else if (orgIds.contains(DEFAULT_ORG_ID)) {
                    clustersByOrg.get(DEFAULT_ORG_ID).add(clusterMeta.getId());
                }
            }
        }

        return clustersByOrg;
    }

}
