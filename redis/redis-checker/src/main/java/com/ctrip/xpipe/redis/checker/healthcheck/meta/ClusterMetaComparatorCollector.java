package com.ctrip.xpipe.redis.checker.healthcheck.meta;

import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.meta.MetaComparator;
import com.ctrip.xpipe.redis.core.meta.MetaComparatorCollector;
import com.ctrip.xpipe.redis.core.meta.comparator.ShardMetaComparator;
import com.ctrip.xpipe.tuple.Pair;

import java.util.ArrayList;
import java.util.List;


public class ClusterMetaComparatorCollector implements MetaComparatorCollector<ShardMeta, Pair<List<RedisMeta>, List<RedisMeta>>> {

    private final List<RedisMeta> redisToAdd = new ArrayList<>();
    private final List<RedisMeta> redisToDelete = new ArrayList<>();

    @Override
    public void visitAdded(ShardMeta added) {
        redisToAdd.addAll(added.getRedises());
    }

    @Override
    public void visitModified(MetaComparator comparator) {
        ShardMetaComparator shardMetaComparator = (ShardMetaComparator) comparator;

        if (shardMetaComparator.isConfigChange()) {
            this.redisToDelete.addAll(shardMetaComparator.getCurrent().getRedises());
            this.redisToAdd.addAll(shardMetaComparator.getFuture().getRedises());
        } else {
            ShardMetaComparatorCollector shardMetaComparatorCollector = new ShardMetaComparatorCollector();
            shardMetaComparator.accept(shardMetaComparatorCollector);
            Pair<List<RedisMeta>, List<RedisMeta>> modifiedRedises=shardMetaComparatorCollector.collect();
            this.redisToDelete.addAll(modifiedRedises.getKey());
            this.redisToAdd.addAll(modifiedRedises.getValue());
        }
    }

    @Override
    public void visitRemoved(ShardMeta removed) {
        this.redisToDelete.addAll(removed.getRedises());
    }


    @Override
    public Pair<List<RedisMeta>,List<RedisMeta>> collect() {
        return new Pair<>(redisToDelete,redisToAdd);
    }
}
