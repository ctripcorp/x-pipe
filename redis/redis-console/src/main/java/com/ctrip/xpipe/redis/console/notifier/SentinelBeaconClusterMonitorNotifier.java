package com.ctrip.xpipe.redis.console.notifier;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.BeaconManager;
import com.ctrip.xpipe.redis.checker.BeaconRouteType;
import com.ctrip.xpipe.redis.checker.config.CheckerDbConfig;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.core.beacon.BeaconSentinelMetaUtil;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.ctrip.xpipe.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * SENTINEL mode Beacon cluster monitor notification.
 */
@Component
@Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
public class SentinelBeaconClusterMonitorNotifier implements BeaconRouteClusterMonitorNotifier {

    private static final Logger logger = LoggerFactory.getLogger(SentinelBeaconClusterMonitorNotifier.class);

    private final BeaconManager beaconManager;
    private final MetaCache metaCache;
    private final ConsoleConfig consoleConfig;
    private final CheckerDbConfig checkerDbConfig;

    @Autowired
    public SentinelBeaconClusterMonitorNotifier(BeaconManager beaconManager, MetaCache metaCache,
                                                ConsoleConfig consoleConfig, CheckerDbConfig checkerDbConfig) {
        this.beaconManager = beaconManager;
        this.metaCache = metaCache;
        this.consoleConfig = consoleConfig;
        this.checkerDbConfig = checkerDbConfig;
    }

    @Override
    public boolean needNotify(String clusterName, String dc, long orgId) {
        if (!checkerDbConfig.shouldSentinelCheck(clusterName)) {
            logger.info("[needNotify][{}][skip] sentinel check excluded", clusterName);
            return false;
        }
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
            ClusterType clusterType = BeaconSentinelMetaUtil.resolveEffectiveClusterType(clusterMeta);
            if (!BeaconSentinelMetaUtil.isSentinelManagedClusterType(clusterType)) {
                return;
            }
            if (BeaconSentinelMetaUtil.isSentinelInterestedDc(clusterMeta, clusterType, dc)) {
                interestedDcs.add(dc);
            }
        });
        return interestedDcs;
    }

}
