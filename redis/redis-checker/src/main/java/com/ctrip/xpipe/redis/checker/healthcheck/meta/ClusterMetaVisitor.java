package com.ctrip.xpipe.redis.checker.healthcheck.meta;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.google.common.collect.Sets;

/**
 * @author chen.zhu
 * <p>
 * Aug 28, 2018
 */
public class ClusterMetaVisitor implements MetaVisitor<ClusterMeta> {

    private ShardMetaVisitor shardMetaVisitor;

    public ClusterMetaVisitor(ShardMetaVisitor shardMetaVisitor) {
        this.shardMetaVisitor = shardMetaVisitor;
    }

    @Override
    public void accept(ClusterMeta clusterMeta) {
        if (ClusterType.lookup(clusterMeta.getType()).supportSingleActiveDC()
                && clusterMeta.getBackupDcs() != null
                && Sets.newHashSet(clusterMeta.getBackupDcs().toUpperCase().split("\\s*,\\s*")).contains(FoundationService.DEFAULT.getDataCenter().toUpperCase())) {
            return;
        }
        for(ShardMeta shard : clusterMeta.getShards().values()) {
            shardMetaVisitor.accept(shard);
        }
    }

}
