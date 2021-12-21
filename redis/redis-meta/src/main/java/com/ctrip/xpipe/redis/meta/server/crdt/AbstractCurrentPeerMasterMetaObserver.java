package com.ctrip.xpipe.redis.meta.server.crdt;

import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.meta.comparator.ClusterMetaComparator;
import com.ctrip.xpipe.redis.meta.server.keeper.impl.AbstractCurrentMetaObserver;

import java.util.Collections;
import java.util.Set;

public abstract class AbstractCurrentPeerMasterMetaObserver extends AbstractCurrentMetaObserver implements TopElement, Observer {

    @Override
    public Set<ClusterType> getSupportClusterTypes() {
        return Collections.singleton(ClusterType.BI_DIRECTION);
    }

    @Override
    protected void handleClusterModified(ClusterMetaComparator comparator) {
        Long clusterDbId = comparator.getCurrent().getDbId();
        for (ShardMeta shardMeta : comparator.getAdded()){
            addShard(clusterDbId, shardMeta.getDbId());
        }
    }

    @Override
    protected void handleClusterDeleted(ClusterMeta clusterMeta) {

    }

    @Override
    protected void handleClusterAdd(ClusterMeta clusterMeta) {
        Long clusterDbId = clusterMeta.getDbId();
        for(ShardMeta shardMeta : clusterMeta.getShards().values()){
            addShard(clusterDbId, shardMeta.getDbId());
        }
    }

    protected abstract void addShard(Long clusterDbId, Long shardDbId);

}
