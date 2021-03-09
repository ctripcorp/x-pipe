package com.ctrip.xpipe.redis.checker.healthcheck.meta;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;

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
                && !clusterMeta.getActiveDc().equalsIgnoreCase(FoundationService.DEFAULT.getDataCenter())) {
            return;
        }
        for(ShardMeta shard : clusterMeta.getShards().values()) {
            shardMetaVisitor.accept(shard);
        }
    }

}
