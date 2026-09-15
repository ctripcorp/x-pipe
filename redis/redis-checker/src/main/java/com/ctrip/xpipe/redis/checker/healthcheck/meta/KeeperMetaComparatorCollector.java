package com.ctrip.xpipe.redis.checker.healthcheck.meta;

import com.ctrip.xpipe.redis.core.entity.InstanceNode;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.meta.MetaComparator;
import com.ctrip.xpipe.redis.core.meta.MetaComparatorCollector;
import com.ctrip.xpipe.redis.core.meta.MetaComparatorVisitor;
import com.ctrip.xpipe.redis.core.meta.comparator.InstanceNodeComparator;
import com.ctrip.xpipe.redis.core.meta.comparator.ShardMetaComparator;
import com.ctrip.xpipe.tuple.Pair;

import java.util.ArrayList;
import java.util.List;

/** Collects Keeper changes from the existing cluster/shard comparator chain. */
public class KeeperMetaComparatorCollector implements MetaComparatorCollector<ShardMeta, Pair<List<KeeperMeta>, List<KeeperMeta>>> {

    private final List<KeeperMeta> keepersToAdd = new ArrayList<>();
    private final List<KeeperMeta> keepersToDelete = new ArrayList<>();

    @Override
    public void visitAdded(ShardMeta added) {
        keepersToAdd.addAll(added.getKeepers());
    }

    @Override
    public void visitModified(MetaComparator comparator) {
        ShardMetaComparator shardComparator = (ShardMetaComparator) comparator;
        if (shardComparator.isConfigChange()) {
            keepersToDelete.addAll(shardComparator.getCurrent().getKeepers());
            keepersToAdd.addAll(shardComparator.getFuture().getKeepers());
            return;
        }
        shardComparator.accept(new KeeperInstanceVisitor());
    }

    @Override
    public void visitRemoved(ShardMeta removed) {
        keepersToDelete.addAll(removed.getKeepers());
    }

    @Override
    public Pair<List<KeeperMeta>, List<KeeperMeta>> collect() {
        return new Pair<>(keepersToDelete, keepersToAdd);
    }

    private class KeeperInstanceVisitor implements MetaComparatorVisitor<InstanceNode> {

        @Override
        public void visitAdded(InstanceNode added) {
            if (added instanceof KeeperMeta) {
                keepersToAdd.add((KeeperMeta) added);
            }
        }

        @Override
        public void visitModified(MetaComparator comparator) {
            InstanceNodeComparator instanceComparator = (InstanceNodeComparator) comparator;
            if (instanceComparator.getCurrent() instanceof KeeperMeta
                    && instanceComparator.getFuture() instanceof KeeperMeta) {
                keepersToDelete.add((KeeperMeta) instanceComparator.getCurrent());
                keepersToAdd.add((KeeperMeta) instanceComparator.getFuture());
            }
        }

        @Override
        public void visitRemoved(InstanceNode removed) {
            if (removed instanceof KeeperMeta) {
                keepersToDelete.add((KeeperMeta) removed);
            }
        }
    }
}
