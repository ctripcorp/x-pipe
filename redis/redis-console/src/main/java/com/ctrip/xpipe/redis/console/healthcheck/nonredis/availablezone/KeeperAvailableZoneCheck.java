package com.ctrip.xpipe.redis.console.healthcheck.nonredis.availablezone;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.checker.model.DcClusterShard;
import com.ctrip.xpipe.redis.console.AbstractCrossDcIntervalAction;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class KeeperAvailableZoneCheck extends AbstractCrossDcIntervalAction {
    @Autowired
    private MetaCache metaCache;

    @Autowired
    private AlertManager alertManager;

    private final List<ALERT_TYPE> alertType = Lists.newArrayList(ALERT_TYPE.KEEPER_IN_SAME_AVAILABLE_ZONE);

    private static final String KEEPER_AVAILABLE_ZONE_CHECK_TYPE = "keeper.available.zone.check";

    @Override
    protected void doAction() {
        XpipeMeta xpipeMeta = metaCache.getXpipeMeta();
        EventMonitor.DEFAULT.logEvent(KEEPER_AVAILABLE_ZONE_CHECK_TYPE, "Do keeper available zone check");

        for (DcMeta dcMeta : xpipeMeta.getDcs().values()) {
            List<AzMeta> azmetas = dcMeta.getAzs();
            if (azmetas == null || azmetas.isEmpty() || azmetas.size() == 1)
                continue;

            List<KeeperContainerMeta> keeperContainers = dcMeta.getKeeperContainers();
            List<DcClusterShard> clusterShards = findKeeperInSameAvailableZones(dcMeta, keeperContainers);
            clusterShards.forEach(clusterShard -> {
                logger.info("[docheck][{}][{}][{}] keepers are in the same available zone",
                        clusterShard.getDcId(), clusterShard.getClusterId(), clusterShard.getShardId());

                EventMonitor.DEFAULT.logEvent(KEEPER_AVAILABLE_ZONE_CHECK_TYPE,
                        String.format("%s-%s-%s", clusterShard.getDcId(), clusterShard.getClusterId(), clusterShard.getShardId()));

            });

            alertForKeeperInSameAvailableZone(dcMeta.getId(), clusterShards);
        }
    }


    private List<DcClusterShard> findKeeperInSameAvailableZones(DcMeta dcMeta, List<KeeperContainerMeta> keeperContainers) {
        List<DcClusterShard> clusterShards = new ArrayList<>();
        Map<String, Long> keeperConatainerAvailableMap = new HashMap<>();

        keeperContainers.forEach(keeperContainerMeta ->{
            logger.debug("[findKeeperInSameAvailableZones][{}]:keeperContainer {}:{} is from available zone {}",
                    dcMeta.getId(), keeperContainerMeta.getIp(), keeperContainerMeta.getPort(), keeperContainerMeta.getAzId());
            keeperConatainerAvailableMap.put(keeperContainerMeta.getIp(), keeperContainerMeta.getAzId());
        });

        for (ClusterMeta clusterMeta : dcMeta.getClusters().values()) {
            if(!ClusterType.lookup(clusterMeta.getType()).supportKeeper())
                continue;

            for (ShardMeta shardMeta : clusterMeta.getShards().values()) {
                if (!isDcClusterShardInDifferentAvailableZones(dcMeta, clusterMeta, shardMeta, keeperConatainerAvailableMap)) {
                    clusterShards.add(new DcClusterShard(dcMeta.getId(), clusterMeta.getId(), shardMeta.getId()));
                }
            }
        }

        return clusterShards;
    }

    private boolean isDcClusterShardInDifferentAvailableZones(DcMeta dcMeta, ClusterMeta clusterMeta, ShardMeta shardMeta, Map<String, Long> keeperConatainerAvailableMap) {
        List<KeeperMeta> keepers = shardMeta.getKeepers();
        if(keepers == null || keepers.size() == 0) return true;

        Set<Long> resultSet = new HashSet<>();
        for (KeeperMeta keeper : keepers) {
            logger.debug("[isDcClusterShardInDifferentAvailableZones][{}][{}][{}]:keeper {}:{} is in available zone {}",
                    dcMeta.getId(), clusterMeta.getId(), shardMeta.getId(), keeper.getIp(), keeper.getPort(), keeperConatainerAvailableMap.get(keeper.getIp()));
            if (!resultSet.add(keeperConatainerAvailableMap.get(keeper.getIp()))) {
                return false;
            }
        }
        return true;
    }

    private void alertForKeeperInSameAvailableZone(String dc, List<DcClusterShard> clusterShards) {
        clusterShards.forEach(clusterShard -> {
            alertManager.alert(dc, clusterShard.getClusterId(), clusterShard.getShardId(), null,
                    ALERT_TYPE.KEEPER_IN_SAME_AVAILABLE_ZONE, "keepers in the same available zone found");
        });
    }


    @Override
    protected List<ALERT_TYPE> alertTypes() {
        return alertType;
    }
}