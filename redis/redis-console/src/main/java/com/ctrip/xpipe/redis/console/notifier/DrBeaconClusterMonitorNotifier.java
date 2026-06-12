package com.ctrip.xpipe.redis.console.notifier;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.BeaconManager;
import com.ctrip.xpipe.redis.checker.BeaconRouteType;
import com.ctrip.xpipe.redis.core.config.ConsoleCommonConfig;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.ctrip.xpipe.utils.StringUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * DR mode Beacon cluster monitor notification.
 */
@Component
@Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
public class DrBeaconClusterMonitorNotifier implements BeaconRouteClusterMonitorNotifier {

    private final BeaconManager beaconManager;
    private final MetaCache metaCache;
    private final ConsoleCommonConfig config;

    @Autowired
    public DrBeaconClusterMonitorNotifier(BeaconManager beaconManager, MetaCache metaCache,
                                          ConsoleCommonConfig config) {
        this.beaconManager = beaconManager;
        this.metaCache = metaCache;
        this.config = config;
    }

    @Override
    public boolean needNotify(String clusterName, String dc, long orgId) {
        if (StringUtil.isEmpty(dc)) {
            return !interestedDcs(clusterName).isEmpty();
        }
        return metaCache.isDcClusterMigratable(clusterName, dc) && isDcInSupportZones(dc);
    }

    @Override
    public void notifyClusterUpdate(String clusterName, String dc, long orgId, String lastModifyTime) {
        ClusterType clusterType = metaCache.getClusterType(clusterName);
        if (StringUtil.isEmpty(dc)) {
            interestedDcs(clusterName).forEach(interestedDc ->
                    beaconManager.registerCluster(clusterName, interestedDc, clusterType, (int) orgId, lastModifyTime,
                            BeaconRouteType.DR));
            return;
        }
        beaconManager.registerCluster(clusterName, dc, clusterType, (int) orgId, lastModifyTime, BeaconRouteType.DR);
    }

    @Override
    public void notifyClusterDelete(String clusterName, String dc, long orgId) {
        ClusterType clusterType = metaCache.getClusterType(clusterName);
        if (StringUtil.isEmpty(dc)) {
            interestedDcs(clusterName).forEach(interestedDc ->
                    beaconManager.unregisterCluster(clusterName, interestedDc, clusterType, (int) orgId,
                            BeaconRouteType.DR));
            return;
        }
        beaconManager.unregisterCluster(clusterName, dc, clusterType, (int) orgId, BeaconRouteType.DR);
    }

    Set<String> interestedDcs(String clusterName) {
        if (metaCache.getXpipeMeta() == null || metaCache.getXpipeMeta().getDcs() == null) {
            return Set.of();
        }

        Set<String> interestedDcs = new LinkedHashSet<>();
        metaCache.getXpipeMeta().getDcs().forEach((dc, dcMeta) -> {
            ClusterMeta clusterMeta = dcMeta.getClusters().get(clusterName);
            if (clusterMeta == null) {
                return;
            }
            if (!dc.equalsIgnoreCase(clusterMeta.getActiveDc())) {
                return;
            }
            if (!metaCache.isDcClusterMigratable(clusterName, dc) || !isDcInSupportZones(dc)) {
                return;
            }
            interestedDcs.add(dc);
        });
        return interestedDcs;
    }

    private boolean isDcInSupportZones(String dc) {
        Set<String> supportZones = config.getBeaconSupportZones();
        if (supportZones.isEmpty()) {
            return true;
        }
        return supportZones.stream().anyMatch(zone -> metaCache.isDcInRegion(dc, zone));
    }

}
