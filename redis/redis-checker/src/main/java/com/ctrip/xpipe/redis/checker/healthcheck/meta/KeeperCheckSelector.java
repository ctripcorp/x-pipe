package com.ctrip.xpipe.redis.checker.healthcheck.meta;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Owns the side-effect-free hard-loading rules for Keeper health-check instances. */
@Component
public class KeeperCheckSelector {

    private final CheckerConfig checkerConfig;
    private final MetaCache metaCache;
    private final String currentDcId;

    @Autowired
    public KeeperCheckSelector(CheckerConfig checkerConfig, MetaCache metaCache) {
        this(checkerConfig, metaCache, FoundationService.DEFAULT.getDataCenter());
    }

    @VisibleForTesting
    public KeeperCheckSelector(CheckerConfig checkerConfig, MetaCache metaCache, String currentDcId) {
        this.checkerConfig = checkerConfig;
        this.metaCache = metaCache;
        this.currentDcId = currentDcId;
    }

    public List<KeeperMeta> select(XpipeMeta xpipeMeta) {
        if (xpipeMeta == null || !checkerConfig.isKeeperDelayCheckEnabled()) {
            return Collections.emptyList();
        }
        List<KeeperMeta> selected = new ArrayList<>();
        for (DcMeta dcMeta : xpipeMeta.getDcs().values()) {
            selected.addAll(select(dcMeta));
        }
        return selected;
    }

    public List<KeeperMeta> select(DcMeta dcMeta) {
        if (dcMeta == null || !checkerConfig.isKeeperDelayCheckEnabled()) {
            return Collections.emptyList();
        }
        List<KeeperMeta> selected = new ArrayList<>();
        for (ClusterMeta clusterMeta : dcMeta.getClusters().values()) {
            selected.addAll(select(clusterMeta));
        }
        return selected;
    }

    public List<KeeperMeta> select(ClusterMeta clusterMeta) {
        if (clusterMeta == null || !checkerConfig.isKeeperDelayCheckEnabled()) {
            return Collections.emptyList();
        }
        List<KeeperMeta> selected = new ArrayList<>();
        for (ShardMeta shardMeta : clusterMeta.getShards().values()) {
            for (KeeperMeta keeperMeta : shardMeta.getKeepers()) {
                if (shouldLoad(keeperMeta)) {
                    selected.add(keeperMeta);
                }
            }
        }
        return selected;
    }

    public boolean shouldLoad(KeeperMeta keeperMeta) {
        if (!checkerConfig.isKeeperDelayCheckEnabled() || keeperMeta == null || keeperMeta.parent() == null) {
            return false;
        }
        ShardMeta shardMeta = keeperMeta.parent();
        ClusterMeta clusterMeta = shardMeta.parent();
        if (clusterMeta == null) {
            return false;
        }
        DcMeta keeperDc = clusterMeta.parent();
        if (keeperDc == null) {
            return false;
        }
        String activeDc = clusterMeta.getActiveDc();
        if (ClusterType.lookup(clusterMeta.getType()) != ClusterType.ONE_WAY
                || StringUtil.isEmpty(activeDc) || StringUtil.isEmpty(currentDcId)
                || !activeDc.equalsIgnoreCase(currentDcId) || StringUtil.isEmpty(keeperDc.getId())) {
            return false;
        }
        return keeperDc.getId().equalsIgnoreCase(activeDc) || !metaCache.isCrossRegion(keeperDc.getId(), activeDc);
    }
}
