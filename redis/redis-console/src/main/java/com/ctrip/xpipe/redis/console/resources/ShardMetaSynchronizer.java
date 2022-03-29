package com.ctrip.xpipe.redis.console.resources;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.monitor.CatEventMonitor;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.model.ShardTbl;
import com.ctrip.xpipe.redis.console.sentinel.SentinelBalanceService;
import com.ctrip.xpipe.redis.console.service.RedisService;
import com.ctrip.xpipe.redis.console.service.ShardService;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
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
    private RedisService redisService;
    private ShardService shardService;
    private SentinelBalanceService sentinelBalanceService;
    private ConsoleConfig consoleConfig;

    public ShardMetaSynchronizer(Set<ShardMeta> added, Set<ShardMeta> removed, Set<MetaComparator> modified, RedisService redisService, ShardService shardService, SentinelBalanceService sentinelBalanceService, ConsoleConfig consoleConfig) {
        this.added = added;
        this.removed = removed;
        this.modified = modified;
        this.redisService = redisService;
        this.shardService = shardService;
        this.sentinelBalanceService = sentinelBalanceService;
        this.consoleConfig = consoleConfig;
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
                        logger.info("[ShardMetaSynchronizer][deleteShard]{}", shardMeta);
                        shardService.deleteShard(((ClusterMeta) shardMeta.parent()).getId(), shardMeta.getId());
                        CatEventMonitor.DEFAULT.logEvent(META_SYNC, String.format("[deleteShard]%s", shardMeta.getId()));
                    } catch (Exception e) {
                        logger.error("[ShardMetaSynchronizer][deleteShard]{}", shardMeta, e);
                    }
                });
        } catch (Exception e) {
            logger.error("[ShardMetaSynchronizer][remove]", e);
        }
    }

    void add() {
        try {
            if (!(added == null || added.isEmpty()))
                added.forEach(shardMeta -> {
                    try {
                        logger.info("[ShardMetaSynchronizer][findOrCreateShardIfNotExist]{}", shardMeta);
                        ClusterMeta clusterMeta = shardMeta.parent();

                        if (consoleConfig.supportSentinelHealthCheck(ClusterType.lookup(clusterMeta.getType()), clusterMeta.getId())) {
                            shardService.findOrCreateShardIfNotExist(((ClusterMeta) shardMeta.parent()).getId(), new ShardTbl().setShardName(shardMeta.getId()).setSetinelMonitorName(shardMeta.getId()), sentinelBalanceService.selectMultiDcSentinels());
                        } else {
                            shardService.findOrCreateShardIfNotExist(((ClusterMeta) shardMeta.parent()).getId(), new ShardTbl().setShardName(shardMeta.getId()).setSetinelMonitorName(shardMeta.getId()), null);
                        }

                        CatEventMonitor.DEFAULT.logEvent(META_SYNC, String.format("[addShard]%s", shardMeta.getId()));
                        new RedisMetaSynchronizer(Sets.newHashSet(shardMeta.getRedises()), null, null, redisService).sync();
                    } catch (Exception e) {
                        logger.error("[ShardMetaSynchronizer][findOrCreateShardIfNotExist]{}", shardMeta, e);
                    }
                });
        } catch (Exception e) {
            logger.error("[ShardMetaSynchronizer][add]", e);
        }
    }

    void update() {
        try {
            if (!(modified == null || modified.isEmpty()))
                modified.forEach(metaComparator -> {
                    ShardMetaComparator shardMetaComparator = (ShardMetaComparator) metaComparator;
                    try {
                        logger.info("[ShardMetaSynchronizer][update]{} -> {}", shardMetaComparator.getCurrent(), shardMetaComparator.getFuture());
                        new RedisMetaSynchronizer(shardMetaComparator.getAdded(), shardMetaComparator.getRemoved(), shardMetaComparator.getMofified(), redisService).sync();
                    } catch (Exception e) {
                        logger.error("[ShardMetaSynchronizer][update]{} -> {}", ((ShardMetaComparator) metaComparator).getCurrent(), ((ShardMetaComparator) metaComparator).getFuture(), e);
                    }
                });
        } catch (Exception e) {
            logger.error("[ShardMetaSynchronizer][update]", e);
        }
    }


}
