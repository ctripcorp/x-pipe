package com.ctrip.xpipe.redis.console.healthcheck.nonredis.sentinelconfig;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.checker.model.DcClusterShard;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.config.ConsoleDbConfig;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.AbstractCrossDcIntervalCheck;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@Component
public class SentinelConfigCheck extends AbstractCrossDcIntervalCheck {

    @Autowired
    private MetaCache metaCache;

    @Autowired
    private AlertManager alertManager;

    @Autowired
    private ConsoleDbConfig consoleDbConfig;

    @Autowired
    private ConsoleConfig consoleConfig;

    private final List<ALERT_TYPE> alertType = Lists.newArrayList(ALERT_TYPE.SENTINEL_CONFIG_MISSING);

    protected void doCheck() {
        XpipeMeta xpipeMeta = metaCache.getXpipeMeta();
        if (null == xpipeMeta) return;

        for (DcMeta dc: xpipeMeta.getDcs().values()) {
            List<DcClusterShard> clusterShards = findUnsafeClusterShardInDc(dc);
            alertForSentinelMissing(dc.getId(), clusterShards);
        }
    }

    private List<DcClusterShard> findUnsafeClusterShardInDc(DcMeta dcMeta) {
        List<DcClusterShard> clusterShards = new ArrayList<>();
        Set<String> whitelist = consoleDbConfig.sentinelCheckWhiteList(false);

        for (ClusterMeta cluster: dcMeta.getClusters().values()) {
            if (whitelist.contains(cluster.getId().toLowerCase())) continue;

            if (!consoleConfig.supportSentinelHealthCheck(ClusterType.lookup(cluster.getType()), cluster.getId()))
                continue;

            for (ShardMeta shard: cluster.getShards().values()) {
                if (!isDcClusterShardSafe(dcMeta, cluster, shard)) {
                    clusterShards.add(new DcClusterShard(dcMeta.getId(), cluster.getId(), shard.getId()));
                }
            }
        }

        return clusterShards;
    }

    private boolean isDcClusterShardSafe(DcMeta dcMeta, ClusterMeta cluster, ShardMeta shard) {
        if (!ClusterType.lookup(cluster.getType()).supportMultiActiveDC()) {
            // sentinel is unnecessary for single active cluster in cross-region dc
            String activeDc = metaCache.getActiveDc(cluster.getId(), shard.getId());
            if (metaCache.isCrossRegion(activeDc, dcMeta.getId())) return true;
        }

        return null != shard.getSentinelId() && !shard.getSentinelId().equals(0L);
    }

    private void alertForSentinelMissing(String dc, List<DcClusterShard> clusterShards) {
        clusterShards.forEach(clusterShard -> {
            alertManager.alert(dc, clusterShard.getClusterId(), clusterShard.getShardId(), null,
                    ALERT_TYPE.SENTINEL_CONFIG_MISSING, "no sentinel set for dc-cluster-shard");
        });
    }

    protected long getIntervalMilli() {
        return consoleConfig.getRedisConfCheckIntervalMilli();
    }

    protected List<ALERT_TYPE> alertTypes() {
        return alertType;
    }

}
