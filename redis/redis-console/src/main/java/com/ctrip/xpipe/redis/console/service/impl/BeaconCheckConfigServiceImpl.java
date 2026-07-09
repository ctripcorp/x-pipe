package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.service.BeaconCheckConfigService;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcClusterShardService;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.beacon.BeaconConstant;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.utils.DateTimeUtils;
import com.ctrip.xpipe.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

@Service
public class BeaconCheckConfigServiceImpl implements BeaconCheckConfigService {

    private static final Logger logger = LoggerFactory.getLogger(BeaconCheckConfigServiceImpl.class);

    private static final Date DEFAULT_OPERATING_UNTIL = BeaconConstant.DEFAULT_OPERATING_UNTIL;

    private final DcClusterShardService dcClusterShardService;

    private final ClusterService clusterService;

    private final ConsoleConfig consoleConfig;

    private final MetaCache metaCache;

    @Autowired
    public BeaconCheckConfigServiceImpl(DcClusterShardService dcClusterShardService,
                                        ClusterService clusterService,
                                        ConsoleConfig consoleConfig,
                                        MetaCache metaCache) {
        this.dcClusterShardService = dcClusterShardService;
        this.clusterService = clusterService;
        this.consoleConfig = consoleConfig;
        this.metaCache = metaCache;
    }

    @Override
    public void stopBeaconCheck(String clusterName, String dc, List<String> shards, int maintainMinutes) throws Exception {
        validateRequest(clusterName, dc, shards);
        validateSentinelBeaconCluster(clusterName, dc);
        Date until = DateTimeUtils.getMinutesLaterThan(new Date(), maintainMinutes);
        int affected = updateOperatingUntil(clusterName, dc, shards, until);
        // 不主动刷新缓存，等待自动刷新生效
        logger.info("[stopBeaconCheck][{}][{}][{}] until {}, affected {}", clusterName, dc, shards, until, affected);
    }

    @Override
    public void startBeaconCheck(String clusterName, String dc, List<String> shards) throws Exception {
        validateRequest(clusterName, dc, shards);
        validateSentinelBeaconCluster(clusterName, dc);
        int affected = updateOperatingUntil(clusterName, dc, shards, DEFAULT_OPERATING_UNTIL);
        // 不主动刷新缓存，等待自动刷新生效
        logger.info("[startBeaconCheck][{}][{}][{}] affected {}", clusterName, dc, shards, affected);
    }

    private void validateSentinelBeaconCluster(String clusterName, String dc) {
        ClusterTbl cluster = clusterService.find(clusterName);
        if (cluster == null) {
            throw new IllegalArgumentException(String.format("cluster %s not found", clusterName));
        }
        if (!consoleConfig.supportSentinelBeacon(cluster.getClusterOrgId(), clusterName)) {
            throw new IllegalArgumentException(String.format("cluster %s is not managed by beacon sentinel mode", clusterName));
        }

        XpipeMeta xpipeMeta = metaCache.getXpipeMeta();
        if (xpipeMeta == null || xpipeMeta.getDcs() == null) {
            throw new IllegalStateException("meta cache not ready");
        }
        DcMeta dcMeta = findDcMeta(xpipeMeta, dc);
        if (dcMeta == null) {
            throw new IllegalArgumentException(String.format("dc %s not found", dc));
        }
        ClusterMeta clusterMeta = dcMeta.getClusters().get(clusterName);
        if (clusterMeta == null) {
            throw new IllegalArgumentException(String.format("cluster %s not found in dc %s", clusterName, dc));
        }
        ClusterType clusterType = resolveEffectiveClusterType(clusterMeta);
        if (!isSentinelManagedClusterType(clusterType)) {
            throw new IllegalArgumentException(String.format("cluster %s type %s is not supported by beacon sentinel mode",
                    clusterName, clusterType));
        }
        if (!isSentinelInterestedDc(clusterMeta, clusterType, dc)) {
            throw new IllegalArgumentException(String.format("dc %s is not a sentinel beacon interested dc for cluster %s",
                    dc, clusterName));
        }
    }

    private DcMeta findDcMeta(XpipeMeta xpipeMeta, String dc) {
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

    private ClusterType resolveEffectiveClusterType(ClusterMeta clusterMeta) {
        ClusterType clusterType = ClusterType.lookup(clusterMeta.getType());
        if (clusterType == ClusterType.HETERO && !StringUtil.isEmpty(clusterMeta.getAzGroupType())) {
            return ClusterType.lookup(clusterMeta.getAzGroupType());
        }
        return clusterType;
    }

    private boolean isSentinelManagedClusterType(ClusterType clusterType) {
        return clusterType == ClusterType.ONE_WAY
                || clusterType == ClusterType.SINGLE_DC
                || clusterType == ClusterType.LOCAL_DC;
    }

    private boolean isSentinelInterestedDc(ClusterMeta clusterMeta, ClusterType clusterType, String dc) {
        if (clusterType.supportMultiActiveDC()) {
            return isDcInClusterDcs(clusterMeta, dc);
        }
        return dc.equalsIgnoreCase(clusterMeta.getActiveDc());
    }

    private boolean isDcInClusterDcs(ClusterMeta clusterMeta, String dc) {
        if (StringUtil.isEmpty(clusterMeta.getDcs()) || StringUtil.isEmpty(dc)) {
            return false;
        }
        return Arrays.stream(clusterMeta.getDcs().split("\\s*,\\s*"))
                .anyMatch(clusterDc -> clusterDc.equalsIgnoreCase(dc));
    }

    private void validateRequest(String clusterName, String dc, List<String> shards) {
        if (StringUtil.isEmpty(clusterName)) {
            throw new IllegalArgumentException("clusterName can not be empty");
        }
        if (StringUtil.isEmpty(dc)) {
            throw new IllegalArgumentException("dc can not be empty");
        }
        if (CollectionUtils.isEmpty(shards)) {
            throw new IllegalArgumentException("shards can not be empty");
        }
        for (String shardName : shards) {
            if (StringUtil.isEmpty(shardName)) {
                throw new IllegalArgumentException("shard name can not be empty");
            }
        }
    }

    private int updateOperatingUntil(String clusterName, String dc, List<String> shards, Date operatingUntil)
            throws Exception {
        return dcClusterShardService.batchUpdateOperatingUntil(dc, clusterName, shards, operatingUntil);
    }
}
