package com.ctrip.xpipe.redis.console.resources;

import com.ctrip.xpipe.monitor.CatEventMonitor;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.service.RedisService;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.console.service.exception.ResourceNotFoundException;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.InstanceNode;
import com.ctrip.xpipe.redis.core.meta.MetaComparator;
import com.ctrip.xpipe.redis.core.meta.MetaSynchronizer;
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
    private Set<InstanceNode> added;
    private Set<InstanceNode> removed;
    private Set<MetaComparator> modified;


    public RedisMetaSynchronizer(Set<InstanceNode> added, Set<InstanceNode> removed, Set<MetaComparator> modified, RedisService redisService) {
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
            for (InstanceNode instanceNode : removed) {
                toDeleted.add(new Pair<>(instanceNode.getIp(), instanceNode.getPort()));
                clusterId = ((ClusterMeta) ((RedisMeta) instanceNode).parent().parent()).getId();
                shardId = ((RedisMeta) instanceNode).parent().getId();
            }
            logger.info("[RedisMetaSynchronizer][deleteRedises]{}", removed);
            redisService.deleteRedises(DcMetaSynchronizer.currentDcId, clusterId, shardId, toDeleted);
            CatEventMonitor.DEFAULT.logEvent(META_SYNC, String.format("[deleteRedises]%s", toDeleted));
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
            List<RedisMeta> toUpdateMaster = new ArrayList<>();
            for (InstanceNode instanceNode : added) {
                toAdded.add(new Pair<>(instanceNode.getIp(), instanceNode.getPort()));
                toUpdateMaster.add((RedisMeta) instanceNode);
                clusterId = ((ClusterMeta) ((RedisMeta) instanceNode).parent().parent()).getId();
                shardId = ((RedisMeta) instanceNode).parent().getId();
            }
            logger.info("[RedisMetaSynchronizer][insertRedises]{}", added);
            redisService.insertRedises(DcMetaSynchronizer.currentDcId, clusterId, shardId, toAdded);
            updateBatchMaster(toUpdateMaster, clusterId, shardId);
            CatEventMonitor.DEFAULT.logEvent(META_SYNC, String.format("[addRedises]%s", toAdded));
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
            List<RedisMeta> futureList = new ArrayList<>();
            for (MetaComparator metaComparator : modified) {
                RedisMeta redisMeta = (RedisMeta) ((RedisComparator) metaComparator).getFuture();
                futureList.add(redisMeta);
                clusterId = ((ClusterMeta) redisMeta.parent().parent()).getId();
                shardId = redisMeta.parent().getId();
            }
            updateBatchMaster(futureList, clusterId, shardId);
            CatEventMonitor.DEFAULT.logEvent(META_SYNC, String.format("[updateBatchMaster]%s", futureList));
        } catch (Exception e) {
            logger.error("[RedisMetaSynchronizer][updateRedises]", e);
        }
    }

    void updateBatchMaster(List<RedisMeta> futureMetaList, String clusterId, String shardId) throws ResourceNotFoundException {
        List<RedisTbl> currentTblList = redisService.findRedisesByDcClusterShard(DcMetaSynchronizer.currentDcId, clusterId, shardId);
        List<RedisTbl> futureTblList = new ArrayList<>();
        for (RedisMeta future : futureMetaList) {
            for (RedisTbl current : currentTblList) {
                if (current.getRedisIp().equals(future.getIp()) && current.getRedisPort() == future.getPort()) {
                    futureTblList.add(current.setMaster(future.isMaster()));
                    break;
                }
            }
        }
        if (!futureTblList.isEmpty()) {
            logger.info("[RedisMetaSynchronizer][updateRedises]{}", futureTblList);
            redisService.updateBatchMaster(futureTblList);
        }
    }

}
