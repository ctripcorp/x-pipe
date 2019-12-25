package com.ctrip.xpipe.redis.console.healthcheck.nonredis.sentinelconfig;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.alert.AlertManager;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.AbstractCrossDcIntervalCheck;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.tuple.Pair;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

@Component
public class SentinelConfigCheck extends AbstractCrossDcIntervalCheck {

    @Autowired
    private MetaCache metaCache;

    @Autowired
    private AlertManager alertManager;

    @Autowired
    private ClusterService clusterService;

    private final List<ALERT_TYPE> alertType = Lists.newArrayList(ALERT_TYPE.SENTINEL_CONFIG_MISSING);

    protected void doCheck() {
        Set<String> dcs = metaCache.getDcs();
        if (null == dcs || dcs.isEmpty()) return;

        for (String dc: dcs) {
            List<Pair<String, String> > clusterShards = findUnsafeClusterShardInDc(dc);
            fixSentinelMissing(dc, clusterShards);
            alertForSentinelMissing(dc, clusterShards);
        }
    }

    private List<Pair<String, String> > findUnsafeClusterShardInDc(String dc) {
        List<Pair<String, String> > clusterShardList = metaCache.getDcClusterShard(dc);
        if (null == clusterShardList || clusterShardList.isEmpty()) return Collections.emptyList();

        return clusterShardList.stream().filter(clusterShard -> {
            String activeDc = metaCache.getActiveDc(clusterShard.getKey(), clusterShard.getValue());
            // sentinel is unnecessary for cross-region dc
            if (metaCache.isCrossRegion(activeDc, dc)) return false;
            Set<HostPort> sentinels = metaCache.getSentinels(dc, clusterShard.getKey(), clusterShard.getValue());
            return null == sentinels || sentinels.isEmpty();
        }).collect(Collectors.toList());
    }

    private void fixSentinelMissing(String dc, List<Pair<String, String> > clusterShards) {
        Set<String> clusterSet = new HashSet<>();
        clusterShards.forEach(clusterShard -> clusterSet.add(clusterShard.getKey()));
        clusterService.reBalanceClusterSentinels(dc, new ArrayList<>(clusterSet));
    }

    private void alertForSentinelMissing(String dc, List<Pair<String, String> > clusterShards) {
        clusterShards.forEach(clusterShard -> {
            alertManager.alert(dc, clusterShard.getKey(), clusterShard.getValue(), null,
                    ALERT_TYPE.SENTINEL_CONFIG_MISSING, "no sentinel set for dc-cluster-shard");
        });
    }

    protected long getIntervalMilli() {
        return consoleConfig.getSentinelConfigCheckIntervalMilli();
    }

    protected List<ALERT_TYPE> alertTypes() {
        return alertType;
    }

}
