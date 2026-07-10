package com.ctrip.xpipe.redis.core.beacon;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.utils.DateTimeUtils;
import com.ctrip.xpipe.utils.StringUtil;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

/**
 * Shared sentinel-beacon meta helpers.
 */
public final class BeaconSentinelMetaUtil {

    private BeaconSentinelMetaUtil() {
    }

    public static ClusterType resolveEffectiveClusterType(ClusterMeta clusterMeta) {
        ClusterType clusterType = ClusterType.lookup(clusterMeta.getType());
        if (clusterType == ClusterType.HETERO && !StringUtil.isEmpty(clusterMeta.getAzGroupType())) {
            return ClusterType.lookup(clusterMeta.getAzGroupType());
        }
        return clusterType;
    }

    public static boolean isSentinelManagedClusterType(ClusterType clusterType) {
        return clusterType == ClusterType.ONE_WAY
                || clusterType == ClusterType.SINGLE_DC
                || clusterType == ClusterType.LOCAL_DC;
    }

    public static boolean isOperatingExcluded(ShardMeta shardMeta) {
        if (shardMeta == null) {
            return false;
        }
        Long operatingUntil = shardMeta.getOperatingUntil();
        return operatingUntil != null
                && operatingUntil > DateTimeUtils.DEFAULT_OPERATING_UNTIL_MILLIS
                && System.currentTimeMillis() < operatingUntil;
    }

    public static boolean isDcInClusterDcs(ClusterMeta clusterMeta, String dc) {
        if (StringUtil.isEmpty(clusterMeta.getDcs()) || StringUtil.isEmpty(dc)) {
            return false;
        }
        return Arrays.stream(clusterMeta.getDcs().split("\\s*,\\s*"))
                .anyMatch(clusterDc -> clusterDc.equalsIgnoreCase(dc));
    }

    public static boolean isSentinelInterestedDc(ClusterMeta clusterMeta, ClusterType clusterType, String dc) {
        if (clusterType.supportMultiActiveDC()) {
            return isDcInClusterDcs(clusterMeta, dc);
        }
        return dc.equalsIgnoreCase(clusterMeta.getActiveDc());
    }

    public static DcMeta findDcMeta(XpipeMeta xpipeMeta, String dc) {
        if (xpipeMeta == null || xpipeMeta.getDcs() == null || StringUtil.isEmpty(dc)) {
            return null;
        }
        DcMeta dcMeta = xpipeMeta.getDcs().get(dc);
        if (dcMeta != null) {
            return dcMeta;
        }
        return xpipeMeta.getDcs().entrySet().stream()
                .filter(entry -> entry.getKey().equalsIgnoreCase(dc))
                .map(entry -> entry.getValue())
                .findFirst()
                .orElse(null);
    }

    public static BeaconSystem resolveBeaconSystemByRouteType(ClusterType clusterType, BeaconRouteType routeType) {
        BeaconSystem beaconSystem = BeaconSystem.findByClusterType(clusterType);
        if (beaconSystem != null) {
            return beaconSystem;
        }
        return BeaconSystem.XPIPE_ONE_WAY;
    }

    public static boolean isBeaconCandidate(DcMeta dcMeta, String clusterName, BeaconRouteType routeType) {
        return isBeaconCandidate(dcMeta, clusterName, routeType, Collections.emptySet());
    }

    public static boolean isBeaconCandidate(DcMeta dcMeta, String clusterName, BeaconRouteType routeType,
                                            Set<String> beaconSupportZones) {
        if (dcMeta == null || StringUtil.isEmpty(clusterName) || dcMeta.getClusters() == null) {
            return false;
        }
        ClusterMeta clusterMeta = dcMeta.getClusters().get(clusterName);
        if (clusterMeta == null) {
            return false;
        }

        ClusterType clusterType = ClusterType.lookup(!StringUtil.isEmpty(clusterMeta.getAzGroupType())
                ? clusterMeta.getAzGroupType() : clusterMeta.getType());

        if (routeType == BeaconRouteType.DR) {
            Set<String> supportZones = beaconSupportZones == null ? Collections.emptySet() : beaconSupportZones;
            if (!supportZones.isEmpty()
                    && supportZones.stream().noneMatch(zone -> zone.equalsIgnoreCase(dcMeta.getZone()))) {
                return false;
            }
            if (!ClusterType.supportClusterMigration(clusterMeta.getType(), clusterMeta.getAzGroupType())) {
                return false;
            }
            if (clusterType.supportSingleActiveDC()
                    && !dcMeta.getId().equalsIgnoreCase(clusterMeta.getActiveDc())) {
                return false;
            }
        } else if (!isSentinelManagedClusterType(clusterType)) {
            return false;
        }

        return resolveBeaconSystemByRouteType(clusterType, routeType) != null;
    }
}
