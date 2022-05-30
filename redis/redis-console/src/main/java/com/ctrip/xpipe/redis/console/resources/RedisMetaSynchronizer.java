package com.ctrip.xpipe.redis.console.resources;

import com.ctrip.xpipe.monitor.CatEventMonitor;
import com.ctrip.xpipe.redis.console.service.RedisService;
import com.ctrip.xpipe.redis.core.entity.Redis;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.MetaComparator;
import com.ctrip.xpipe.redis.core.meta.MetaSynchronizer;
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
        remove();
        add();
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
            for (Redis redis : added) {
                toAdded.add(new Pair<>(redis.getIp(), redis.getPort()));
                toUpdateMaster.add((RedisMeta) redis);
                clusterId = ((RedisMeta) redis).parent().parent().getId();
                shardId = ((RedisMeta) redis).parent().getId();
            }
            logger.info("[RedisMetaSynchronizer][insertRedises]{}", added);
            redisService.insertRedises(DcMetaSynchronizer.currentDcId, clusterId, shardId, toAdded);
            CatEventMonitor.DEFAULT.logEvent(META_SYNC, String.format("[addRedises]%s", toAdded));
        } catch (Exception e) {
            logger.error("[RedisMetaSynchronizer][insertRedises]", e);
        }
    }

}
