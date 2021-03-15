package com.ctrip.xpipe.redis.console.service.meta.impl;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.api.migration.auto.data.MonitorGroupMeta;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.meta.BeaconMetaService;
import com.ctrip.xpipe.redis.console.service.meta.ClusterMetaService;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
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

    private DcService dcService;

    private ClusterMetaService clusterMetaService;

    @Autowired
    public BeaconMetaServiceImpl(MetaCache metaCache, DcService dcService, ClusterMetaService clusterMetaService) {
        this.metaCache = metaCache;
        this.dcService = dcService;
        this.clusterMetaService = clusterMetaService;
    }

    @Override
    public Set<MonitorGroupMeta> buildBeaconGroups(String cluster) {
        XpipeMeta xpipeMeta = metaCache.getXpipeMeta();
        if (null == xpipeMeta) return Collections.emptySet();

        Map<String, ClusterMeta> dcClusterMetas = new HashMap<>();
        xpipeMeta.getDcs().forEach((dc, dcMeta) -> {
            ClusterMeta clusterMeta = dcMeta.getClusters().get(cluster);
            if (null != clusterMeta) {
                dcClusterMetas.put(dc, clusterMeta);
            }
        });

        return buildBeaconGroups(dcClusterMetas);
    }

    @Override
    public Set<MonitorGroupMeta> buildCurrentBeaconGroups(String cluster) {
        List<DcTbl> relatedDcs = dcService.findClusterRelatedDc(cluster);
        if (null == relatedDcs) throw new IllegalArgumentException("no related dcs found for " + cluster);

        List<String> dcs = relatedDcs.stream().map(DcTbl::getDcName).collect(Collectors.toList());
        Map<String, ClusterMeta> dcClusters = dcs.stream().collect(Collectors.toMap(
                dc -> dc,
                dc -> clusterMetaService.getClusterMeta(dc, cluster)
        ));
        return buildBeaconGroups(dcClusters);
    }

    @Override
    public boolean compareMetaWithXPipe(String clusterName, Set<MonitorGroupMeta> beaconGroups) {
        Set<MonitorGroupMeta> metaFromXPipe = buildBeaconGroups(clusterName);
        if (metaFromXPipe.isEmpty()) return false;

        return metaFromXPipe.equals(beaconGroups);
    }

    private Set<MonitorGroupMeta> buildBeaconGroups(Map<String, ClusterMeta> dcClusterMetas) {
        Set<MonitorGroupMeta> groups = new HashSet<>();
        dcClusterMetas.forEach((dc, clusterMeta) -> {
            String activeDc = clusterMeta.getActiveDc();
            // no register cross region dcs to beacon
            if (!metaCache.isCrossRegion(activeDc, dc)) {
                clusterMeta.getShards().forEach((shard, shardMeta) -> {
                    Set<HostPort> nodes = shardMeta.getRedises().stream()
                            .map(redisMeta -> new HostPort(redisMeta.getIp(), redisMeta.getPort()))
                            .collect(Collectors.toSet());
                    String groupName = String.join(BEACON_GROUP_SEPARATOR, shard, dc);
                    groups.add(new MonitorGroupMeta(groupName, dc, nodes, activeDc.equals(dc)));
                });
            }
        });

        return groups;
    }

    @VisibleForTesting
    public void setMetaCache(MetaCache metaCache) {
        this.metaCache = metaCache;
    }

}
