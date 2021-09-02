package com.ctrip.xpipe.redis.core.meta.comparator;

import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.google.common.collect.Sets;
import org.unidal.tuple.Triple;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class DcSyncMetaComparator extends AbstractMetaComparator<ClusterMeta, DcChange> {

    private DcMeta current, future;

    public DcSyncMetaComparator(DcMeta current, DcMeta future) {
        this.current = current;
        this.future = future;
    }

    public void compare() {

        Triple<Set<String>, Set<String>, Set<String>> result = getDiff(current.getClusters().keySet(), future.getClusters().keySet());

        Set<String> addedClusterIds = result.getFirst();
        Set<String> intersectionClusterIds = result.getMiddle();
        Set<String> deletedClusterIds = result.getLast();

        for (String clusterId : addedClusterIds) {
            added.add(future.findCluster(clusterId));
        }

        for (String clusterId : deletedClusterIds) {
            removed.add(current.findCluster(clusterId));
        }

        for (String clusterId : intersectionClusterIds) {
            ClusterMeta currentMeta = current.findCluster(clusterId);
            ClusterMeta futureMeta = future.findCluster(clusterId);
            if (clusterChanged(currentMeta, futureMeta)) {
                ClusterSyncMetaComparator clusterMetaComparator = new ClusterSyncMetaComparator(currentMeta, futureMeta);
                clusterMetaComparator.compare();
                modified.add(clusterMetaComparator);
            }
        }
    }

    boolean clusterChanged(ClusterMeta current, ClusterMeta future) {
        return clusterInfoChanged(current, future) || clusterShardsChanged(current, future);
    }

    boolean clusterInfoChanged(ClusterMeta current, ClusterMeta future) {
        return !(Objects.equals(current.getId(), future.getId()) &&
                Objects.equals(current.getOrgId(), future.getOrgId()) &&
                Objects.equals(current.getAdminEmails(), future.getAdminEmails()) &&
                Objects.equals(current.getType(), future.getType()));
    }

    boolean clusterShardsChanged(ClusterMeta current, ClusterMeta future) {
        Map<String, ShardMeta> currentShards = current.getShards();
        Map<String, ShardMeta> futureShards = future.getShards();

        Set<String> currentShardNames = currentShards.keySet();
        Set<String> futureShardNames = futureShards.keySet();

        if (!Objects.equals(currentShardNames, futureShardNames))
            return true;

        for (String shardName : currentShards.keySet()) {
            if (shardChanged(currentShards.get(shardName), futureShards.get(shardName)))
                return true;
        }

        return false;
    }

    boolean shardChanged(ShardMeta current, ShardMeta future) {
        return !Objects.equals(Sets.newHashSet(current.getRedises()), Sets.newHashSet(future.getRedises()));
    }

    @Override
    public String idDesc() {

        if (current != null) {
            return current.getId();
        }
        if (future != null) {
            return future.getId();
        }
        return null;
    }
}
