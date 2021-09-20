package com.ctrip.xpipe.redis.core.meta.comparator;

import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.google.common.collect.Sets;
import org.unidal.tuple.Triple;

import java.util.Objects;
import java.util.Set;

public class ClusterSyncMetaComparator extends AbstractMetaComparator<ShardMeta, ClusterChange> {

    private ClusterMeta current, future;

    public ClusterSyncMetaComparator(ClusterMeta current, ClusterMeta future) {
        this.current = current;
        this.future = future;
    }

    @Override
    public void compare() {

        Triple<Set<String>, Set<String>, Set<String>> result = getDiff(current.getShards().keySet(), future.getShards().keySet());

        for (String shardId : result.getFirst()) {
            added.add(future.findShard(shardId));
        }

        for (String shardId : result.getLast()) {
            removed.add(current.findShard(shardId));
        }

        for (String shardId : result.getMiddle()) {
            ShardMeta currentMeta = current.findShard(shardId);
            ShardMeta futureMeta = future.findShard(shardId);
            if (shardChanged(currentMeta, futureMeta)) {
                ShardMetaComparator comparator = new ShardMetaComparator(currentMeta, futureMeta);
                comparator.compare();
                modified.add(comparator);
            }
        }
    }

    boolean shardChanged(ShardMeta current, ShardMeta future) {
        return !Objects.equals(Sets.newHashSet(current.getRedises()), Sets.newHashSet(future.getRedises()));
    }

    public ClusterMeta getCurrent() {
        return current;
    }

    public ClusterMeta getFuture() {
        return future;
    }

    @Override
    public String idDesc() {
        return current.getId();
    }
}
