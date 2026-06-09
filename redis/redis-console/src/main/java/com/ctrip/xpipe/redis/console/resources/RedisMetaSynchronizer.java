package com.ctrip.xpipe.redis.console.resources;

import com.ctrip.xpipe.monitor.CatEventMonitor;
import com.ctrip.xpipe.redis.console.cache.AzCache;
import com.ctrip.xpipe.redis.console.service.RedisService;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.InstanceNode;
import com.ctrip.xpipe.redis.core.meta.MetaComparator;
import com.ctrip.xpipe.redis.core.meta.MetaSynchronizer;
import com.ctrip.xpipe.redis.core.meta.comparator.InstanceNodeComparator;
import com.ctrip.xpipe.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class RedisMetaSynchronizer implements MetaSynchronizer {
    private static Logger logger = LoggerFactory.getLogger(RedisMetaSynchronizer.class);
    protected RedisService redisService;
    protected AzCache azCache;
    private Set<InstanceNode> added;
    private Set<InstanceNode> removed;
    private Set<MetaComparator> modified;
    private String dcId;


    public RedisMetaSynchronizer(Set<InstanceNode> added, Set<InstanceNode> removed,
                                 Set<MetaComparator> modified, RedisService redisService,
                                 AzCache azCache, String dcId
    ) {
        this.added = added;
        this.removed = removed;
        this.modified = modified;
        this.redisService = redisService;
        this.azCache = azCache;
        this.dcId = dcId;
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
            redisService.deleteRedises(dcId, clusterId, shardId, toDeleted);
            CatEventMonitor.DEFAULT.logEvent(META_SYNC, String.format("[deleteRedises]%s", toDeleted));
        } catch (Exception e) {
            logger.error("[RedisMetaSynchronizer][deleteRedises]", e);
        }
    }

    void add() {
        try {
            if (added == null || added.isEmpty())
                return;
            for (InstanceNode instanceNode : added) {
                try {
                    RedisMeta redisMeta = (RedisMeta) instanceNode;
                    String clusterId = ((ClusterMeta) redisMeta.parent().parent()).getId();
                    String shardId = redisMeta.parent().getId();
                    List<Pair<String, Integer>> single = new ArrayList<>();
                    single.add(new Pair<>(instanceNode.getIp(), instanceNode.getPort()));

                    Long azId = null;
                    if (azCache != null && redisMeta.getAz() != null) {
                        try {
                            azId = azCache.find(redisMeta.getAz()).getId();
                        } catch (Exception e) {
                            logger.warn("[RedisMetaSynchronizer][add] failed to get azId for az {}", redisMeta.getAz(), e);
                        }
                    }

                    logger.info("[RedisMetaSynchronizer][insertRedises]{}", instanceNode);
                    redisService.insertRedises(dcId, clusterId, shardId, single, azId);
                } catch (Exception e) {
                    logger.error("[RedisMetaSynchronizer][insertRedises]{}", instanceNode, e);
                }
            }
            CatEventMonitor.DEFAULT.logEvent(META_SYNC, String.format("[addRedises]%s", added));
        } catch (Exception e) {
            logger.error("[RedisMetaSynchronizer][insertRedises]", e);
        }
    }

    void update() {
        try {
            if (modified == null || modified.isEmpty())
                return;
            Map<String, Long> addressAzMap = new HashMap<>();
            String clusterId = null;
            String shardId = null;
            for (MetaComparator metaComparator : modified) {
                InstanceNodeComparator comparator = (InstanceNodeComparator) metaComparator;
                RedisMeta future = (RedisMeta) comparator.getFuture();
                RedisMeta current = (RedisMeta) comparator.getCurrent();

                String futureAz = future.getAz();
                String currentAz = current.getAz();
                if (Objects.equals(futureAz, currentAz)) {
                    continue;
                }

                if (clusterId == null) {
                    clusterId = ((ClusterMeta) future.parent().parent()).getId();
                    shardId = future.parent().getId();
                }

                Long azId = null;
                if (azCache != null && futureAz != null) {
                    try {
                        azId = azCache.find(futureAz).getId();
                    } catch (Exception e) {
                        logger.warn("[RedisMetaSynchronizer][update] failed to get azId for az {}", futureAz, e);
                    }
                }

                String addr = future.getIp() + ":" + future.getPort();
                addressAzMap.put(addr, azId);
                logger.info("[RedisMetaSynchronizer][update]{} az: {} -> {}, azId={}", future.desc(), currentAz, futureAz, azId);
            }
            if (!addressAzMap.isEmpty()) {
                redisService.updateRedisesAz(dcId, clusterId, shardId, addressAzMap);
            }
        } catch (Exception e) {
            logger.error("[RedisMetaSynchronizer][update]", e);
        }
    }

}
