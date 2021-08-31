package com.ctrip.xpipe.redis.console.resources;

import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.service.RedisService;
import com.ctrip.xpipe.redis.core.entity.Redis;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.MetaComparator;
import com.ctrip.xpipe.redis.core.meta.MetaSynchronizer;
import com.ctrip.xpipe.redis.core.meta.comparator.RedisComparator;
import com.ctrip.xpipe.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
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
        remove();
        add();
        update();
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
            logger.info("[RedisMetaSynchronizer][deleteRedises]{}", removed);
            redisService.deleteRedises(DcMetaSynchronizer.currentDcId, clusterId, shardId, toDeleted);
        } catch (Exception e) {
            logger.error("[RedisMetaSynchronizer][deleteRedises]", e);
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
            logger.info("[RedisMetaSynchronizer][insertRedises]{}", added);
            redisService.insertRedises(DcMetaSynchronizer.currentDcId, clusterId, shardId, toAdded);
        } catch (Exception e) {
            logger.error("[RedisMetaSynchronizer][insertRedises]", e);
        }
    }

    void update() {
        try {
            if (modified == null || modified.isEmpty())
                return;

            String clusterId = "";
            String shardId = "";
            List<RedisMeta> futureMetaList = new ArrayList<>();
            for (MetaComparator metaComparator : modified) {
                RedisMeta redisMeta = (RedisMeta) ((RedisComparator) metaComparator).getFuture();
                futureMetaList.add(redisMeta);
                clusterId = redisMeta.parent().parent().getId();
                shardId = redisMeta.parent().getId();
            }

            List<RedisTbl> currentTblList = redisService.findRedisesByDcClusterShard(DcMetaSynchronizer.currentDcId, clusterId, shardId);
            List<RedisTbl> futureTblList = new ArrayList<>();
            for (RedisMeta future : futureMetaList) {
                for (RedisTbl current : currentTblList) {
                    if (current.getRedisIp().equals(future.getIp()) || current.getRedisPort() == future.getPort()) {
                        if (shouldUpdate(future, current)) {
                            futureTblList.add(current.setMaster(future.isMaster()));
                        }
                        break;
                    }
                }
            }
            logger.info("[RedisMetaSynchronizer][updateRedises]{}", futureTblList);
            redisService.updateBatchMaster(futureTblList);
        } catch (Exception e) {
            logger.error("[RedisMetaSynchronizer][updateRedises]", e);
        }
    }

    boolean shouldUpdate(RedisMeta future, RedisTbl current) {
        return !(Objects.equals(current.getRedisIp(), future.getIp()) &&
                Objects.equals(current.getRedisPort(), future.getPort()) &&
                Objects.equals(current.isMaster(), future.isMaster()));
    }
}
