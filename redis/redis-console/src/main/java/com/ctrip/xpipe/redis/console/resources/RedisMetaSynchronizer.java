package com.ctrip.xpipe.redis.console.resources;

import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.model.ShardModel;
import com.ctrip.xpipe.redis.core.meta.MetaSynchronizer;
import com.ctrip.xpipe.redis.console.service.RedisService;
import com.ctrip.xpipe.redis.core.entity.Redis;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.MetaComparator;
import com.ctrip.xpipe.redis.core.meta.comparator.RedisComparator;
import com.ctrip.xpipe.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class RedisMetaSynchronizer implements MetaSynchronizer {
    private static Logger logger = LoggerFactory.getLogger(RedisMetaSynchronizer.class);
    protected RedisService redisService;
    private Set<Redis> added;
    private Set<Redis> removed;
    private Set<MetaComparator> modified;

    public RedisMetaSynchronizer(Set<Redis> added, Set<Redis> removed, Set<MetaComparator> modified, RedisService redisService) {
        this.added = added;
        this.removed = removed;
        this.modified = modified;
        this.redisService = redisService;
    }

    public void sync() {
        try {
            remove();
            add();
            update();
        } catch (Exception e) {
            logger.error("RedisMetaSynchronizer sync error", e);
        }
    }

    void remove() {
        try {
            if (removed == null || removed.isEmpty())
                return;
            String clusterId = "";
            String shardId = "";
            List<Pair<String, Integer>> toDeleted = new ArrayList<>();
            for (Redis redis : removed) {
                toDeleted.add(new Pair<>(redis.getIp(), redis.getPort()));
                clusterId = ((RedisMeta) redis).parent().parent().getId();
                shardId = ((RedisMeta) redis).parent().getId();
            }
            redisService.deleteRedises(DcMetaSynchronizer.currentDcId, clusterId, shardId, toDeleted);
        } catch (Exception e) {
            logger.error("RedisMetaSynchronizer remove error", e);
        }
    }

    void add() {
        try {
            if (added == null || added.isEmpty())
                return;
            String clusterId = "";
            String shardId = "";
            List<Pair<String, Integer>> toAdded = new ArrayList<>();
            for (Redis redis : added) {
                toAdded.add(new Pair<>(redis.getIp(), redis.getPort()));
                clusterId = ((RedisMeta) redis).parent().parent().getId();
                shardId = ((RedisMeta) redis).parent().getId();
            }
            redisService.insertRedises(DcMetaSynchronizer.currentDcId, clusterId, shardId, toAdded);
        } catch (Exception e) {
            logger.error("RedisMetaSynchronizer add error", e);
        }
    }

    void update() {
        try {
            if (modified == null || modified.isEmpty())
                return;
            ShardModel shardModel = new ShardModel();
            String clusterId = "";
            String shardId = "";
            List<RedisMeta> futureList = new ArrayList<>();
            for (MetaComparator metaComparator : modified) {
                RedisMeta redisMeta = (RedisMeta) ((RedisComparator) metaComparator).getFuture();
                futureList.add(redisMeta);
                clusterId = redisMeta.parent().parent().getId();
                shardId = redisMeta.parent().getId();
            }

            List<RedisTbl> targetList = redisService.findAllByDcClusterShard(DcMetaSynchronizer.currentDcId, clusterId, shardId);

            for (RedisMeta target : futureList) {
                for (RedisTbl existed : targetList) {
                    if (existed.getRedisIp().equals(target.getIp()) || existed.getRedisPort() == target.getPort()) {
                        if (shouldUpdate(target, existed)) {
                            shardModel.addRedis(existed.setMaster(target.isMaster()));
                        }
                        break;
                    }
                }
            }

            redisService.updateRedises(DcMetaSynchronizer.currentDcId, clusterId, shardId, shardModel);
        } catch (Exception e) {
            logger.error("RedisMetaSynchronizer update error", e);
        }
    }

    boolean shouldUpdate(RedisMeta future, RedisTbl current) {
        return !(current.getRedisIp().equals(future.getIp()) &&
                current.getRedisPort() == future.getPort() &&
                current.isMaster() == future.isMaster());
    }
}
