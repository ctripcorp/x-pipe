package com.ctrip.xpipe.redis.core.beacon;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.utils.StringUtil;

import java.util.Arrays;

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
}
