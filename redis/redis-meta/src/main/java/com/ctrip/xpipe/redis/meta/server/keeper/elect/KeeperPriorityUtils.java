package com.ctrip.xpipe.redis.meta.server.keeper.elect;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Map;

/**
 * Normalize keeper priority at Meta load/update (D28).
 * Callers of {@code DefaultDcMetaCache} / enriched survive lists should not need
 * scattered null→default defense.
 */
public final class KeeperPriorityUtils {

    public static final int DEFAULT_PRIORITY = 1;
    public static final String METRIC_PRIORITY_MISSING = "keeper.priority.missing";
    public static final String METRIC_PRIORITY_ALL_ZERO = "keeper.priority.all_zero";

    private static final Logger logger = LoggerFactory.getLogger(KeeperPriorityUtils.class);

    private KeeperPriorityUtils() {
    }

    public static void normalizeDcMeta(DcMeta dcMeta) {
        if (dcMeta == null || CollectionUtils.isEmpty(dcMeta.getClusters())) {
            return;
        }
        for (ClusterMeta clusterMeta : dcMeta.getClusters().values()) {
            normalizeClusterMeta(clusterMeta);
        }
    }

    public static void normalizeClusterMeta(ClusterMeta clusterMeta) {
        if (clusterMeta == null) {
            return;
        }
        Map<String, ShardMeta> allShards = clusterMeta.getAllShards();
        if (CollectionUtils.isEmpty(allShards)) {
            return;
        }
        for (ShardMeta shardMeta : allShards.values()) {
            normalizeShardKeepers(clusterMeta.getId(), shardMeta);
        }
    }

    /**
     * Per-shard: {@code null → 1}; if every keeper is explicit {@code 0}, rewrite all to {@code 1}.
     */
    public static void normalizeShardKeepers(String clusterId, ShardMeta shardMeta) {
        if (shardMeta == null) {
            return;
        }
        List<KeeperMeta> keepers = shardMeta.getKeepers();
        if (CollectionUtils.isEmpty(keepers)) {
            return;
        }

        boolean hasPositive = false;
        for (KeeperMeta keeperMeta : keepers) {
            Integer priority = keeperMeta.getPriority();
            if (priority == null) {
                keeperMeta.setPriority(DEFAULT_PRIORITY);
                logger.warn("[normalize][null→{}]cluster={},shard={},keeper={}",
                        DEFAULT_PRIORITY, clusterId, shardMeta.getId(), keeperMeta);
                EventMonitor.DEFAULT.logEvent(METRIC_PRIORITY_MISSING, "meta.load");
                hasPositive = true;
            } else if (priority > 0) {
                hasPositive = true;
            }
        }

        if (!hasPositive) {
            for (KeeperMeta keeperMeta : keepers) {
                keeperMeta.setPriority(DEFAULT_PRIORITY);
            }
            logger.warn("[normalize][all-zero→{}]cluster={},shard={},keepers={}",
                    DEFAULT_PRIORITY, clusterId, shardMeta.getId(), keepers);
            EventMonitor.DEFAULT.logEvent(METRIC_PRIORITY_ALL_ZERO, "meta.load");
        }
    }
}
