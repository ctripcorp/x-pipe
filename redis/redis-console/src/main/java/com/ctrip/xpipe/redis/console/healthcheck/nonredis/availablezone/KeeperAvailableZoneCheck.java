package com.ctrip.xpipe.redis.console.healthcheck.nonredis.availablezone;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.checker.model.DcClusterShard;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.AbstractCrossDcIntervalCheck;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.dianping.cat.Cat;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.entity.AzMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;

import java.util.*;

@Component
public class KeeperAvailableZoneCheck extends AbstractCrossDcIntervalCheck {
    @Autowired
    private MetaCache metaCache;

    @Autowired
    private AlertManager alertManager;

    private final List<ALERT_TYPE> alertType = Lists.newArrayList(ALERT_TYPE.KEEPER_IN_DIFFERENT_AVAILABLE_ZONE);

    private static final String KEEPER_AVAILABLE_ZONE_CHECK_TYPE = "keeper.availablezone.check";

    @Override
    protected void doCheck() {
        XpipeMeta xpipeMeta = metaCache.getXpipeMeta();
        for (DcMeta dcMeta : xpipeMeta.getDcs().values()) {
            List<AzMeta> azmetas = dcMeta.getAzs();
            if (azmetas == null || azmetas.isEmpty())
                continue;

            List<KeeperContainerMeta> keeperContainers = dcMeta.getKeeperContainers();
            List<DcClusterShard> clusterShards = findKeeperInSameAvailableZones(dcMeta, keeperContainers);
            clusterShards.forEach(clusterShard -> {
                logger.debug("[findKeeperInSameAvailableZones] keeper from dc {} cluster {} shard {} are in the same available zone",
                        clusterShard.getDcId(), clusterShard.getClusterId(), clusterShard.getShardId());

                EventMonitor.DEFAULT.logEvent(KEEPER_AVAILABLE_ZONE_CHECK_TYPE,
                        String.format("%s-%s-%s", clusterShard.getDcId(), clusterShard.getClusterId(), clusterShard.getShardId()))

            });

            alertForKeeperInSameAvailableZone(dcMeta.getId(), clusterShards);
        }
    }


    private List<DcClusterShard> findKeeperInSameAvailableZones(DcMeta dcMeta, List<KeeperContainerMeta> keeperContainers) {
        List<DcClusterShard> clusterShards = new ArrayList<>();
        Map<String, Long> keeperConatainerAvailableMap = new HashMap<>();

        keeperContainers.forEach(keeperContainerMeta ->{
            logger.debug("[findKeeperInSameAvailableZones]:keeperContainer {}:{} is from available zone {}",
                    keeperContainerMeta.getIp(), keeperContainerMeta.getPort(), keeperContainerMeta.getAzId());
            keeperConatainerAvailableMap.put(keeperContainerMeta.getIp(), keeperContainerMeta.getAzId());
        });

        for (ClusterMeta clusterMeta : dcMeta.getClusters().values()) {
            for (ShardMeta shardMeta : clusterMeta.getShards().values()) {
                if (!isDcClusterShardInDifferentAvailableZones(shardMeta, keeperConatainerAvailableMap)) {
                    clusterShards.add(new DcClusterShard(dcMeta.getId(), clusterMeta.getId(), shardMeta.getId()));
                }
            }
        }

        return clusterShards;
    }

    private boolean isDcClusterShardInDifferentAvailableZones(ShardMeta shardMeta, Map<String, Long> keeperConatainerAvailableMap) {
        List<KeeperMeta> keepers = shardMeta.getKeepers();
        Set<Long> resultSet = new HashSet<>();

        for (KeeperMeta keeper : keepers) {
            logger.debug("[isDcClusterShardInDifferentAvailableZones]:keeper {}:{} from shard {} is in available zone {}",
                     keeper.getIp(), keeper.getPort(), shardMeta.getId(), keeperConatainerAvailableMap.get(keeper.getIp()));
            if (!resultSet.add(keeperConatainerAvailableMap.get(keeper.getIp()))) {
                return false;
            }
        }
        return true;
    }

    private void alertForKeeperInSameAvailableZone(String dc, List<DcClusterShard> clusterShards) {
        clusterShards.forEach(clusterShard -> {
            alertManager.alert(dc, clusterShard.getClusterId(), clusterShard.getShardId(), null,
                    ALERT_TYPE.KEEPER_IN_DIFFERENT_AVAILABLE_ZONE, "keepers in the same available zone found");
        });
    }


    @Override
    protected List<ALERT_TYPE> alertTypes() {
        return alertType;
    }
}