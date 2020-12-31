package com.ctrip.xpipe.redis.console.service.migration.impl;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.controller.api.migrate.meta.GroupStatusVO;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.redis.console.service.migration.BeaconMetaComparator;
import com.ctrip.xpipe.redis.console.service.migration.exception.ClusterNotFoundException;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author lishanglin
 * date 2020/12/28
 */
@Component
public class BeaconMetaComparatorImpl implements BeaconMetaComparator {

    private MetaCache metaCache;

    private static final String BEACON_GROUP_SEPARATOR = "\\+";

    @Autowired
    public BeaconMetaComparatorImpl(MetaCache metaCache) {
        this.metaCache = metaCache;
    }

    public boolean compareWithXPipe(String clusterName, List<GroupStatusVO> beaconClusterMeta) throws ClusterNotFoundException {
        XpipeMeta xpipeMeta = metaCache.getXpipeMeta();
        if (null == xpipeMeta) return false;

        Map<String, Map<String, Set<HostPort>>> metaFromXPipe = buildMetaFromXPipe(clusterName);
        if (metaFromXPipe.isEmpty()) return false;

        Map<String, Map<String, Set<HostPort>>> metaFromBeacon = buildMetaFromBeacon(beaconClusterMeta);
        if (metaFromBeacon.isEmpty()) return false;

        return metaFromXPipe.equals(metaFromBeacon);
    }

    private Map<String, Map<String, Set<HostPort>>> buildMetaFromXPipe(String clusterName) {
        XpipeMeta xpipeMeta = metaCache.getXpipeMeta();
        if (null == xpipeMeta) return Collections.emptyMap();

        Map<String, Map<String, Set<HostPort>>> cluster = new HashMap<>();
        xpipeMeta.getDcs().forEach((dc, dcMeta) -> {
            ClusterMeta clusterMeta = dcMeta.getClusters().get(clusterName);
            if (null == clusterMeta) return;

            String activeDc = dcMeta.getClusters().get(clusterName).getActiveDc();
            // no register cross region dcs to beacon
            if (!metaCache.isCrossRegion(dc, activeDc)) {
                cluster.put(dc, new HashMap<>());
                clusterMeta.getShards().forEach((shard, shardMeta) -> {
                    Set<HostPort> redisSet = shardMeta.getRedises().stream()
                            .map(redisMeta -> new HostPort(redisMeta.getIp(), redisMeta.getPort()))
                            .collect(Collectors.toSet());
                    cluster.get(dc).put(shard, redisSet);
                });
            }
        });

        return cluster;
    }

    private Map<String, Map<String, Set<HostPort>>> buildMetaFromBeacon(List<GroupStatusVO> beaconClusterMeta) {
        Map<String, Map<String, Set<HostPort>>> cluster = new HashMap<>();

        beaconClusterMeta.forEach(groupStatusVO -> {
            String[] infos = groupStatusVO.getName().split(BEACON_GROUP_SEPARATOR);
            String shard = infos[0];
            String dc = infos[1];

            if (!cluster.containsKey(dc)) cluster.put(dc, new HashMap<>());
            cluster.get(dc).put(shard, new HashSet<>(groupStatusVO.getNodes()));
        });

        return cluster;
    }

}
