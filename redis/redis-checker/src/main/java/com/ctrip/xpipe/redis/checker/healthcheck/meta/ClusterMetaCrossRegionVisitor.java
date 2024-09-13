package com.ctrip.xpipe.redis.checker.healthcheck.meta;

import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;

public class ClusterMetaCrossRegionVisitor implements MetaVisitor<ClusterMeta>{

    private ShardMetaCrossRegionVisitor shardMetaVisitor;

    public ClusterMetaCrossRegionVisitor(ShardMetaCrossRegionVisitor shardMetaVisitor) {
        this.shardMetaVisitor = shardMetaVisitor;
    }

    @Override
    public void accept(ClusterMeta clusterMeta) {
        for(ShardMeta shard : clusterMeta.getShards().values()) {
            shardMetaVisitor.accept(shard);
        }
    }
}
