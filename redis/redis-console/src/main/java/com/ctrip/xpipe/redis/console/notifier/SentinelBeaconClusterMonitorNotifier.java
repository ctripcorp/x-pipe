package com.ctrip.xpipe.redis.console.notifier;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.BeaconManager;
import com.ctrip.xpipe.redis.checker.BeaconRouteType;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.ctrip.xpipe.utils.StringUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * SENTINEL mode Beacon cluster monitor notification.
 */
@Component
@Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
public class SentinelBeaconClusterMonitorNotifier implements BeaconRouteClusterMonitorNotifier {

    private final BeaconManager beaconManager;
    private final MetaCache metaCache;
    private final ConsoleConfig consoleConfig;

    @Autowired
    public SentinelBeaconClusterMonitorNotifier(BeaconManager beaconManager, MetaCache metaCache,
                                                ConsoleConfig consoleConfig) {
        this.beaconManager = beaconManager;
        this.metaCache = metaCache;
        this.consoleConfig = consoleConfig;
    }

    @Override
    public boolean needNotify(String clusterName, String dc, long orgId) {
        if (!consoleConfig.supportSentinelBeacon(orgId, clusterName)) {
            return false;
        }
        Set<String> interestedDcs = interestedDcs(clusterName);
        if (StringUtil.isEmpty(dc)) {
            return !interestedDcs.isEmpty();
        }
        return interestedDcs.contains(dc);
    }

    @Override
    public void notifyClusterUpdate(String clusterName, String dc, long orgId, String lastModifyTime) {
        ClusterType clusterType = metaCache.getClusterType(clusterName);
        if (StringUtil.isEmpty(dc)) {
            interestedDcs(clusterName).forEach(interestedDc ->
                    beaconManager.registerCluster(clusterName, interestedDc, clusterType, (int) orgId, lastModifyTime,
                            BeaconRouteType.SENTINEL));
            return;
        }
        beaconManager.registerCluster(clusterName, dc, clusterType, (int) orgId, lastModifyTime, BeaconRouteType.SENTINEL);
    }

    @Override
    public void notifyClusterDelete(String clusterName, String dc, long orgId) {
        ClusterType clusterType = metaCache.getClusterType(clusterName);
        if (StringUtil.isEmpty(dc)) {
            interestedDcs(clusterName).forEach(interestedDc ->
                    beaconManager.unregisterCluster(clusterName, interestedDc, clusterType, (int) orgId,
                            BeaconRouteType.SENTINEL));
            return;
        }
        beaconManager.unregisterCluster(clusterName, dc, clusterType, (int) orgId, BeaconRouteType.SENTINEL);
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
            ClusterType clusterType = resolveEffectiveClusterType(clusterMeta);
            if (!isSentinelManagedClusterType(clusterType)) {
                return;
            }
            if (clusterType.supportMultiActiveDC()) {
                if (isDcInClusterDcs(clusterMeta, dc)) {
                    interestedDcs.add(dc);
                }
                return;
            }
            if (dc.equalsIgnoreCase(clusterMeta.getActiveDc())) {
                interestedDcs.add(dc);
            }
        });
        return interestedDcs;
    }

    private boolean isSentinelManagedClusterType(ClusterType clusterType) {
        return clusterType == ClusterType.ONE_WAY
                || clusterType == ClusterType.SINGLE_DC
                || clusterType == ClusterType.LOCAL_DC;
    }

    private ClusterType resolveEffectiveClusterType(ClusterMeta clusterMeta) {
        ClusterType clusterType = ClusterType.lookup(clusterMeta.getType());
        if (clusterType == ClusterType.HETERO && !StringUtil.isEmpty(clusterMeta.getAzGroupType())) {
            return ClusterType.lookup(clusterMeta.getAzGroupType());
        }
        return clusterType;
    }

    private boolean isDcInClusterDcs(ClusterMeta clusterMeta, String dc) {
        if (StringUtil.isEmpty(clusterMeta.getDcs()) || StringUtil.isEmpty(dc)) {
            return false;
        }
        return Arrays.stream(clusterMeta.getDcs().split("\\s*,\\s*"))
                .anyMatch(clusterDc -> clusterDc.equalsIgnoreCase(dc));
    }

}
