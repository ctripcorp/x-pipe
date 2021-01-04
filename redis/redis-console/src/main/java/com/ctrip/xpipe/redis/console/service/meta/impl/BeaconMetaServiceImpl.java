package com.ctrip.xpipe.redis.console.service.meta.impl;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.model.beacon.BeaconGroupModel;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.redis.console.service.meta.BeaconMetaService;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author lishanglin
 * date 2020/12/31
 */
@Service
public class BeaconMetaServiceImpl implements BeaconMetaService {

    private MetaCache metaCache;

    @Autowired
    public BeaconMetaServiceImpl(MetaCache metaCache) {
        this.metaCache = metaCache;
    }

    @Override
    public Set<BeaconGroupModel> buildBeaconGroups(String cluster) {
        XpipeMeta xpipeMeta = metaCache.getXpipeMeta();
        if (null == xpipeMeta) return Collections.emptySet();

        Set<BeaconGroupModel> groups = new HashSet<>();
        xpipeMeta.getDcs().forEach((dc, dcMeta) -> {
            ClusterMeta clusterMeta = dcMeta.getClusters().get(cluster);
            if (null == clusterMeta) return;

            String activeDc = dcMeta.getClusters().get(cluster).getActiveDc();
            // no register cross region dcs to beacon
            if (!metaCache.isCrossRegion(dc, activeDc)) {
                clusterMeta.getShards().forEach((shard, shardMeta) -> {
                    Set<HostPort> nodes = shardMeta.getRedises().stream()
                            .map(redisMeta -> new HostPort(redisMeta.getIp(), redisMeta.getPort()))
                            .collect(Collectors.toSet());
                    String groupName = String.join(BEACON_GROUP_SEPARATOR, shard, dc);
                    groups.add(new BeaconGroupModel(groupName, dc, nodes, activeDc.equals(dc)));
                });
            }
        });

        return groups;
    }

    @Override
    public boolean compareMetaWithXPipe(String clusterName, Set<BeaconGroupModel> beaconGroups) {
        Set<BeaconGroupModel> metaFromXPipe = buildBeaconGroups(clusterName);
        if (metaFromXPipe.isEmpty()) return false;

        return metaFromXPipe.equals(beaconGroups);
    }

    @VisibleForTesting
    public void setMetaCache(MetaCache metaCache) {
        this.metaCache = metaCache;
    }

}
