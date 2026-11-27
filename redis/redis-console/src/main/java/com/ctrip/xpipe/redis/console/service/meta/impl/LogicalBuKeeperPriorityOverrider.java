package com.ctrip.xpipe.redis.console.service.meta.impl;

import com.ctrip.xpipe.redis.console.model.KeepercontainerTbl;
import com.ctrip.xpipe.redis.console.service.KeeperContainerService;
import com.ctrip.xpipe.redis.console.service.LogicalBuService;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.keeper.KeeperDiskTypeUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Applies LOGICAL_BU_TBL.priority to TFS keepers when meta is handed to MetaServer.
 *
 * <p>Rule: a keeper whose keepercontainer is TFS and belongs to a logical BU with non-NULL priority
 * gets that priority; every other keeper keeps REDIS_TBL.keeper_priority.
 *
 * <p>Overrides are loaded per meta build and never cached: a stale value would make MetaServer
 * elect back and forth between TFS and BM. Load failures propagate so the build fails instead of
 * silently publishing keeper_priority.
 */
@Component
public class LogicalBuKeeperPriorityOverrider {

    @Autowired
    @Lazy
    private LogicalBuService logicalBuService;

    @Autowired
    @Lazy
    private KeeperContainerService keeperContainerService;

    /** keepercontainerId → BU priority, only TFS containers whose BU priority is non-NULL. */
    public Map<Long, Integer> loadOverrides() {
        Map<Long, Integer> buPriorities = logicalBuService.findAllPriorities();
        if (buPriorities == null || buPriorities.isEmpty()) {
            return Collections.emptyMap();
        }
        List<KeepercontainerTbl> containers = keeperContainerService.findAll();
        if (containers == null || containers.isEmpty()) {
            return Collections.emptyMap();
        }
        return buildOverrides(buPriorities, containers);
    }

    public ClusterMeta override(ClusterMeta clusterMeta) {
        apply(clusterMeta, loadOverrides());
        return clusterMeta;
    }

    public DcMeta override(DcMeta dcMeta) {
        apply(dcMeta, loadOverrides());
        return dcMeta;
    }

    static Map<Long, Integer> buildOverrides(Map<Long, Integer> buPriorities, List<KeepercontainerTbl> containers) {
        Map<Long, Integer> overrides = new HashMap<>();
        for (KeepercontainerTbl container : containers) {
            // findAll() returns deleted containers too (and duplicates, harmless for a map)
            if (container.isDeleted() || !KeeperDiskTypeUtils.isTfs(container.getKeepercontainerDiskType())) {
                continue;
            }
            Integer priority = buPriorities.get(container.getLogicalBuId());
            if (priority != null) {
                overrides.put(container.getKeepercontainerId(), priority);
            }
        }
        return overrides;
    }

    public static void apply(DcMeta dcMeta, Map<Long, Integer> overrides) {
        if (dcMeta == null || overrides.isEmpty() || dcMeta.getClusters() == null) {
            return;
        }
        for (ClusterMeta clusterMeta : dcMeta.getClusters().values()) {
            apply(clusterMeta, overrides);
        }
    }

    public static void apply(ClusterMeta clusterMeta, Map<Long, Integer> overrides) {
        if (clusterMeta == null || overrides.isEmpty()) {
            return;
        }
        Map<String, ShardMeta> shards = clusterMeta.getAllShards();
        if (shards == null) {
            return;
        }
        for (ShardMeta shardMeta : shards.values()) {
            if (shardMeta.getKeepers() == null) {
                continue;
            }
            for (KeeperMeta keeperMeta : shardMeta.getKeepers()) {
                if (keeperMeta.getKeeperContainerId() == null) {
                    continue;
                }
                Integer priority = overrides.get(keeperMeta.getKeeperContainerId());
                if (priority != null) {
                    keeperMeta.setPriority(priority);
                }
            }
        }
    }
}
