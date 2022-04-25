package com.ctrip.xpipe.redis.checker.healthcheck.meta;

import com.ctrip.xpipe.redis.core.entity.Redis;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.MetaComparator;
import com.ctrip.xpipe.redis.core.meta.MetaComparatorCollector;
import com.ctrip.xpipe.redis.core.meta.comparator.RedisComparator;
import com.ctrip.xpipe.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class ShardMetaComparatorCollector implements MetaComparatorCollector<Redis, Pair<List<RedisMeta>, List<RedisMeta>>> {
    private static final Logger logger = LoggerFactory.getLogger(ShardMetaComparatorCollector.class);

    private final List<RedisMeta> toAdd = new ArrayList<>();
    private final List<RedisMeta> toDelete = new ArrayList<>();

    @Override
    public void visitAdded(Redis added) {
        if (added instanceof RedisMeta) {
            toAdd.add((RedisMeta) added);
        }
    }

    @Override
    public void visitModified(MetaComparator comparator) {
        logger.info("[visitModified][redis] {}", comparator);
        RedisComparator redisComparator = (RedisComparator) comparator;
        Redis future = redisComparator.getFuture();
        if (future instanceof RedisMeta) {
            this.toDelete.add((RedisMeta) redisComparator.getFuture());
            this.toAdd.add((RedisMeta) redisComparator.getFuture());
        }
    }

    @Override
    public void visitRemoved(Redis removed) {
        if(removed instanceof RedisMeta) {
            toDelete.add((RedisMeta) removed);
        }
    }

    @Override
    public Pair<List<RedisMeta>, List<RedisMeta>> collect() {
        return new Pair<>(toDelete, toAdd);
    }

}
