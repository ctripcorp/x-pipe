package com.ctrip.xpipe.redis.meta.server.crdt;

import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.meta.comparator.ClusterMetaComparator;
import com.ctrip.xpipe.redis.meta.server.crdt.event.RemotePeerMasterChangeEvent;
import com.ctrip.xpipe.redis.meta.server.keeper.impl.AbstractCurrentMetaObserver;
import com.ctrip.xpipe.tuple.Pair;
import com.google.common.collect.Sets;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public abstract class AbstractPeerMasterMetaObserver extends AbstractCurrentMetaObserver {

    @Override
    public Set<ClusterType> getSupportClusterTypes() {
        return Collections.singleton(ClusterType.BI_DIRECTION);
    }

    @Override
    public void update(Object args, Observable observable) {
        if (args instanceof RemotePeerMasterChangeEvent) {
            RemotePeerMasterChangeEvent event = (RemotePeerMasterChangeEvent) args;
            handleRemotePeerMasterChange(event.getDcId(), event.getClusterId(), event.getShardId());
            return;
        }

        super.update(args, observable);
    }

    @Override
    protected void handleClusterModified(ClusterMetaComparator comparator) {
        ClusterMeta currentCluster = comparator.getCurrent();
        ClusterMeta futureCluster = comparator.getFuture();

        if (!currentCluster.getDcs().equalsIgnoreCase(futureCluster.getDcs())) {
            Pair<Set<String>, Set<String> > dcsDiff = compareRelatedDcs(comparator.getCurrent().getDcs(), comparator.getFuture().getDcs());

            for (ShardMeta shardMeta : currentCluster.getShards().values()) {
                handleDcsAdded(currentCluster.getId(), shardMeta.getId(), dcsDiff.getKey());
                handleDcsDeleted(currentCluster.getId(), shardMeta.getId(), dcsDiff.getValue());
            }
        }
    }

    // return Pair[dcsAdded, dcsDeleted]
    private Pair<Set<String>, Set<String> > compareRelatedDcs(String currentDcsDesc, String futureDcsDesc) {
        Set<String> currentDcs = Sets.newHashSet(currentDcsDesc.toLowerCase().split("\\s*,\\s*"));
        Set<String> futureDcs = Sets.newHashSet(futureDcsDesc.toLowerCase().split("\\s*,\\s*"));
        Set<String> retainDcs = new HashSet<>(currentDcs);
        retainDcs.retainAll(futureDcs);
        currentDcs.removeAll(retainDcs);
        futureDcs.removeAll(retainDcs);

        return Pair.of(futureDcs, currentDcs);
    }

    protected abstract void handleDcsAdded(String clusterId, String shardId, Set<String> dcsAdded);

    protected abstract void handleDcsDeleted(String clusterId, String shardId, Set<String> dcsDeleted);

    protected abstract void handleRemotePeerMasterChange(String dcId, String clusterId, String shardId);

}
