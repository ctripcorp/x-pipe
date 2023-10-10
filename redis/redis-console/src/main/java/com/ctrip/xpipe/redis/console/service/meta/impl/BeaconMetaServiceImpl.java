package com.ctrip.xpipe.redis.console.service.meta.impl;

import com.ctrip.xpipe.api.migration.auto.data.MonitorGroupMeta;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.exception.DataNotFoundException;
import com.ctrip.xpipe.redis.console.service.meta.BeaconMetaService;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.utils.StringUtil;
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

    public ConsoleConfig config;

    @Autowired
    public BeaconMetaServiceImpl(MetaCache metaCache, ConsoleConfig config) {
        this.metaCache = metaCache;
        this.config = config;
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
        XpipeMeta xpipeMeta = metaCache.getXpipeMeta();
        if (null == xpipeMeta) throw new DataNotFoundException("meta not ready");

        Map<String, ClusterMeta> dcClusterMetas = new HashMap<>();
        xpipeMeta.getDcs().values().forEach(dcMeta -> {
            if (dcMeta.getClusters().containsKey(cluster)) {
                dcClusterMetas.put(dcMeta.getId(), dcMeta.getClusters().get(cluster));
            }
        });

        if (dcClusterMetas.isEmpty()) throw new DataNotFoundException("no related dcs found for " + cluster);
        return buildBeaconGroups(dcClusterMetas);
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
            if (isDcNeeded(dc, clusterMeta)) {
                clusterMeta.getShards().forEach((shard, shardMeta) -> {
                    Set<HostPort> nodes = shardMeta.getRedises().stream()
                        .map(redisMeta -> new HostPort(redisMeta.getIp(), redisMeta.getPort()))
                        .collect(Collectors.toSet());
                    String groupName = String.join(BEACON_GROUP_SEPARATOR, shard, dc);
                    groups.add(new MonitorGroupMeta(groupName, dc, nodes, dc.equalsIgnoreCase(clusterMeta.getActiveDc())));
                });
            }
        });

        return groups;
    }

    private boolean isDcNeeded(String dc, ClusterMeta clusterMeta) {
        if (ClusterType.isSameClusterType(clusterMeta.getType(), ClusterType.ONE_WAY)) {
            String activeDc = clusterMeta.getActiveDc();
            // no register cross region dcs to beacon
            return !metaCache.isCrossRegion(activeDc, dc);
        } else if (ClusterType.isSameClusterType(clusterMeta.getType(), ClusterType.HETERO)) {
            return !ClusterType.isSameClusterType(clusterMeta.getAzGroupType(), ClusterType.SINGLE_DC);
        } else {
            XpipeMeta xpipeMeta = metaCache.getXpipeMeta();
            String supportZone = config.getBeaconSupportZone();
            if (null == xpipeMeta || StringUtil.isEmpty(supportZone)) return false;

            return supportZone.equalsIgnoreCase(clusterMeta.parent().getZone());
        }
    }

    @VisibleForTesting
    public void setMetaCache(MetaCache metaCache) {
        this.metaCache = metaCache;
    }

}
