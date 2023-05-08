package com.ctrip.xpipe.redis.console.healthcheck.nonredis.beacon;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.BeaconManager;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.AbstractCrossDcIntervalAction;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * @author lishanglin
 * date 2022/4/8
 */
@Component
public class BeaconBiClusterMonitorRegister extends AbstractCrossDcIntervalAction {

    private MetaCache metaCache;

    private BeaconManager beaconManager;

    private ConsoleConfig config;


    @Autowired
    public BeaconBiClusterMonitorRegister(MetaCache metaCache, BeaconManager beaconManager, ConsoleConfig config) {
        this.metaCache = metaCache;
        this.beaconManager = beaconManager;
        this.config = config;
    }

    @Override
    protected void doAction() {
        Set<String> clusters = config.getClustersSupportBiMigration();
        for (String cluster: clusters) {
            logger.debug("[doCheck][{}] register", cluster);
            ClusterMeta clusterMeta = findClusterMeta(cluster);
            if (null == clusterMeta || !ClusterType.isSameClusterType(clusterMeta.getType(), ClusterType.BI_DIRECTION)) {
                logger.info("[doCheck][{}][skip]", cluster);
                continue;
            }
            beaconManager.registerCluster(cluster, ClusterType.BI_DIRECTION, clusterMeta.getOrgId());
        }
    }

    private ClusterMeta findClusterMeta(String clusterId) {
        XpipeMeta xpipeMeta = metaCache.getXpipeMeta();
        if (null == xpipeMeta) return null;

        for (DcMeta dcMeta : xpipeMeta.getDcs().values()) {
            if (!dcMeta.getClusters().containsKey(clusterId)) continue;
            return dcMeta.getClusters().get(clusterId);
        }

        return null;
    }

    @Override
    protected List<ALERT_TYPE> alertTypes() {
        return Collections.emptyList();
    }

    @Override
    protected long getIntervalMilli() {
        return config.getClusterHealthCheckInterval();
    }
}
