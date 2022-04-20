package com.ctrip.xpipe.redis.checker.healthcheck.meta;

import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.MetaComparator;
import com.ctrip.xpipe.redis.core.meta.MetaComparatorVisitor;
import com.ctrip.xpipe.redis.core.meta.comparator.InstanceNodeComparator;
import com.ctrip.xpipe.redis.core.meta.comparator.ShardMetaComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * @author chen.zhu
 * <p>
 * Aug 28, 2018
 */
public class ClusterMetaComparatorVisitor implements MetaComparatorVisitor<ShardMeta> {

    private static final Logger logger = LoggerFactory.getLogger(ClusterMetaComparatorVisitor.class);

    private Consumer<RedisMeta> redisAdd;

    private Consumer<RedisMeta> redisDelete;

    private Consumer<RedisMeta> redisChanged;

    private BiConsumer<ShardMeta, ShardMeta> shardConfigChanged;

    public ClusterMetaComparatorVisitor(Consumer<RedisMeta> redisAdd, Consumer<RedisMeta> redisDelete,
                                        Consumer<RedisMeta> redisChanged, BiConsumer<ShardMeta, ShardMeta> shardConfigChanged) {
        this.redisAdd = redisAdd;
        this.redisDelete = redisDelete;
        this.redisChanged = redisChanged;
        this.shardConfigChanged = shardConfigChanged;
    }

    @Override
    public void visitAdded(ShardMeta added) {
        new ShardMetaVisitor(new RedisMetaVisitor(redisAdd)).accept(added);
    }

    @Override
    public void visitModified(MetaComparator comparator) {
        ShardMetaComparator shardMetaComparator = (ShardMetaComparator)comparator;

        if (shardMetaComparator.isConfigChange()) {
            shardConfigChanged.accept(shardMetaComparator.getCurrent(), shardMetaComparator.getFuture());
        } else {
            shardMetaComparator.accept(new MetaComparatorVisitor<InstanceNode>() {
                @Override
                public void visitAdded(InstanceNode added) {
                    if(added instanceof RedisMeta) {
                        redisAdd.accept((RedisMeta) added);
                    }
                }

                @Override
                public void visitModified(MetaComparator comparator) {
                    logger.info("[visitModified][redis] {}", comparator);
                    InstanceNodeComparator instanceNodeComparator = (InstanceNodeComparator) comparator;
                    InstanceNode future = instanceNodeComparator.getFuture();
                    if (future instanceof RedisMeta) {
                        redisChanged.accept((RedisMeta) instanceNodeComparator.getFuture());
                    }
                }

                @Override
                public void visitRemoved(InstanceNode removed) {
                    if(removed instanceof RedisMeta) {
                        redisDelete.accept((RedisMeta) removed);
                    }
                }
            });
        }
    }

    @Override
    public void visitRemoved(ShardMeta removed) {
        new ShardMetaVisitor(new RedisMetaVisitor(redisDelete)).accept(removed);
    }
}
