package com.ctrip.xpipe.redis.console.resources;

import com.ctrip.xpipe.redis.console.model.ShardTbl;
import com.ctrip.xpipe.redis.console.service.RedisService;
import com.ctrip.xpipe.redis.console.service.ShardService;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.meta.MetaComparator;
import com.ctrip.xpipe.redis.core.meta.MetaSynchronizer;
import com.ctrip.xpipe.redis.core.meta.comparator.ShardMetaComparator;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

public class ShardMetaSynchronizer implements MetaSynchronizer {
    private static Logger logger = LoggerFactory.getLogger(ShardMetaSynchronizer.class);
    private Set<ShardMeta> added;
    private Set<ShardMeta> removed;
    private Set<MetaComparator> modified;
    protected RedisService redisService;
    protected ShardService shardService;

    public ShardMetaSynchronizer(Set<ShardMeta> added, Set<ShardMeta> removed, Set<MetaComparator> modified, RedisService redisService, ShardService shardService) {
        this.added = added;
        this.removed = removed;
        this.modified = modified;
        this.redisService = redisService;
        this.shardService = shardService;
    }

    public void sync() {
        remove();
        add();
        update();
    }

    void remove() {
        try {
            if (!(removed == null || removed.isEmpty()))
                removed.forEach(shardMeta -> {
                    try {
                        shardService.deleteShard(shardMeta.parent().getId(), shardMeta.getId());
                    } catch (Exception e) {
                        logger.error("ShardMetaSynchronizer.remove:{}", shardMeta, e);
                    }
                });
        } catch (Exception e) {
            logger.error("ShardMetaSynchronizer.remove", e);
        }
    }

    void add() {
        try {
            if (!(added == null || added.isEmpty()))
                added.forEach(shardMeta -> {
                    try {
                        shardService.createShard(shardMeta.parent().getId(), new ShardTbl().setShardName(shardMeta.getId()).setSetinelMonitorName(shardMeta.getSentinelMonitorName()), null);
                        new RedisMetaSynchronizer(Sets.newHashSet(shardMeta.getRedises()), null, null, redisService).sync();
                    } catch (Exception e) {
                        logger.error("ShardMetaSynchronizer.add:{}", shardMeta.toString(), e);
                    }
                });
        } catch (Exception e) {
            logger.error("ShardMetaSynchronizer.add", e);
        }
    }

    void update() {
        try {
            if (!(modified == null || modified.isEmpty()))
                modified.forEach(metaComparator -> {
                    try {
                        ShardMetaComparator shardMetaComparator = (ShardMetaComparator) metaComparator;
                        new RedisMetaSynchronizer(shardMetaComparator.getAdded(), shardMetaComparator.getRemoved(), shardMetaComparator.getMofified(), redisService).sync();
                    } catch (Exception e) {
                        logger.error("ShardMetaSynchronizer.update:{}->{}", ((ShardMetaComparator) metaComparator).getCurrent(), ((ShardMetaComparator) metaComparator).getFuture(), e);
                    }
                });
        } catch (Exception e) {
            logger.error("ShardMetaSynchronizer.update", e);
        }
    }

}
