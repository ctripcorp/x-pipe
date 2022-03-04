package com.ctrip.xpipe.redis.checker.healthcheck.meta;

import com.ctrip.xpipe.redis.core.entity.Redis;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.meta.MetaComparator;
import com.ctrip.xpipe.redis.core.meta.MetaComparatorVisitor;
import com.ctrip.xpipe.redis.core.meta.comparator.RedisComparator;
import com.ctrip.xpipe.redis.core.meta.comparator.ShardMetaComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    public ClusterMetaComparatorVisitor(Consumer<RedisMeta> redisAdd, Consumer<RedisMeta> redisDelete, Consumer<RedisMeta> redisChanged) {
        this.redisAdd = redisAdd;
        this.redisDelete = redisDelete;
        this.redisChanged = redisChanged;
    }

    @Override
    public void visitAdded(ShardMeta added) {
        new ShardMetaVisitor(new RedisMetaVisitor(redisAdd)).accept(added);
    }

    @Override
    public void visitModified(MetaComparator comparator) {
        ShardMetaComparator shardMetaComparator = (ShardMetaComparator)comparator;

        if (shardMetaComparator.metaChange()) {
            reloadShard();
        } else {
            shardMetaComparator.accept(new MetaComparatorVisitor<Redis>() {
                @Override
                public void visitAdded(Redis added) {
                    if(added instanceof RedisMeta) {
                        redisAdd.accept((RedisMeta) added);
                    }
                }

                @Override
                public void visitModified(MetaComparator comparator) {
                    logger.info("[visitModified][redis] {}", comparator);
                    RedisComparator redisComparator = (RedisComparator) comparator;
                    reloadRedis();
                }

                @Override
                public void visitRemoved(Redis removed) {
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
