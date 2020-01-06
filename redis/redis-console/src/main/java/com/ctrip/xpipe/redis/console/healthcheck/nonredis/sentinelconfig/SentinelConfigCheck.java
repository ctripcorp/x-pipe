package com.ctrip.xpipe.redis.console.healthcheck.nonredis.sentinelconfig;

import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.alert.AlertManager;
import com.ctrip.xpipe.redis.console.config.ConsoleDbConfig;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.AbstractCrossDcIntervalCheck;
import com.ctrip.xpipe.redis.console.model.DcClusterShard;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class SentinelConfigCheck extends AbstractCrossDcIntervalCheck {

    @Autowired
    private MetaCache metaCache;

    @Autowired
    private AlertManager alertManager;

    @Autowired
    private ConsoleDbConfig consoleDbConfig;

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
            for (ShardMeta shard: cluster.getShards().values()) {
                if (whitelist.contains(cluster.getId())) continue;
                String activeDc = metaCache.getActiveDc(cluster.getId(), shard.getId());
                // sentinel is unnecessary for cross-region dc
                if (metaCache.isCrossRegion(activeDc, dcMeta.getId())) continue;
                if (null != shard.getSentinelId() && !shard.getSentinelId().equals(0L)) continue;
                clusterShards.add(new DcClusterShard(dcMeta.getId(), cluster.getId(), shard.getId()));
            }
        }

        return clusterShards;
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
