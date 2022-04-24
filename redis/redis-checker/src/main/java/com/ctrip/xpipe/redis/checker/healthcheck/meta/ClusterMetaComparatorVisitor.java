package com.ctrip.xpipe.redis.checker.healthcheck.meta;

import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.meta.MetaComparator;
import com.ctrip.xpipe.redis.core.meta.MetaComparatorVisitor;
import com.ctrip.xpipe.redis.core.meta.comparator.ShardMetaComparator;

import java.util.ArrayList;
import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Aug 28, 2018
 */
public class ClusterMetaComparatorVisitor implements MetaComparatorVisitor<ShardMeta> {

    private List<ShardMeta> shardsToAdd = new ArrayList<>();
    private List<ShardMeta> shardsToDelete = new ArrayList<>();
    private List<RedisMeta> redisToAdd = new ArrayList<>();
    private List<RedisMeta> redisToDelete = new ArrayList<>();

    @Override
    public void visitAdded(ShardMeta added) {
        this.shardsToAdd.add(added);
    }

    @Override
    public void visitModified(MetaComparator comparator) {
        ShardMetaComparator shardMetaComparator = (ShardMetaComparator) comparator;

        if (shardMetaComparator.isConfigChange()) {
            this.shardsToDelete.add(shardMetaComparator.getCurrent());
            this.shardsToAdd.add(shardMetaComparator.getFuture());
        } else {
            ShardMetaComparatorVisitor shardMetaComparatorVisitor = new ShardMetaComparatorVisitor();
            shardMetaComparator.accept(shardMetaComparatorVisitor);
            this.redisToAdd = shardMetaComparatorVisitor.getToAdd();
            this.redisToDelete = shardMetaComparatorVisitor.getToDelete();
        }
    }

    @Override
    public void visitRemoved(ShardMeta removed) {
        this.shardsToDelete.add(removed);
    }

    public List<ShardMeta> getShardsToAdd() {
        return shardsToAdd;
    }

    public List<ShardMeta> getShardsToDelete() {
        return shardsToDelete;
    }

    public List<RedisMeta> getRedisToAdd() {
        return redisToAdd;
    }

    public List<RedisMeta> getRedisToDelete() {
        return redisToDelete;
    }
}
