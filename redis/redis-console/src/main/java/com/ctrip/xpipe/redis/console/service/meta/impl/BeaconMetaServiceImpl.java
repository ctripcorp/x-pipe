package com.ctrip.xpipe.redis.console.service.meta.impl;

import com.ctrip.xpipe.api.migration.auto.data.MonitorGroupMeta;
import com.ctrip.xpipe.api.migration.auto.data.MonitorShardMeta;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.service.meta.BeaconMetaService;
import com.ctrip.xpipe.redis.core.config.ConsoleCommonConfig;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    private static final Logger logger = LoggerFactory.getLogger(BeaconMetaServiceImpl.class);

    private MetaCache metaCache;

    private ConsoleCommonConfig config;

    @Autowired
    public BeaconMetaServiceImpl(MetaCache metaCache, ConsoleCommonConfig config) {
        this.metaCache = metaCache;
        this.config = config;
    }

    @Override
    public Set<MonitorGroupMeta> buildDrBeaconGroups(String cluster, String dc) {
        ClusterMeta dcClusterMeta = getClusterMeta(cluster, dc);
        if (dcClusterMeta == null) {
            return Collections.emptySet();
        }

        if (resolveEffectiveClusterType(dcClusterMeta) != ClusterType.ONE_WAY) {
            return Collections.emptySet();
        }

        String activeDc = dcClusterMeta.getActiveDc();
        if (StringUtil.isEmpty(activeDc) || !isDcInSupportZones(activeDc)) {
            return Collections.emptySet();
        }

        Set<MonitorGroupMeta> groups = new HashSet<>();
        for (String scopeDc : resolveSameRegionDcs(dcClusterMeta)) {
            ClusterMeta scopeClusterMeta = getClusterMeta(cluster, scopeDc);
            if (scopeClusterMeta == null) {
                continue;
            }
            scopeClusterMeta.getShards().forEach((shard, shardMeta) -> {
                Set<HostPort> nodes = shardMeta.getRedises().stream()
                        .map(redisMeta -> new HostPort(redisMeta.getIp(), redisMeta.getPort()))
                        .collect(Collectors.toSet());
                String groupName = String.join(BEACON_GROUP_SEPARATOR, shard, scopeDc);
                groups.add(new MonitorGroupMeta(groupName, scopeDc, nodes, scopeDc.equalsIgnoreCase(activeDc)));
            });
        }
        return groups;
    }

    @Override
    public Set<MonitorShardMeta> buildSentinelBeaconShards(String cluster, String dc, Map<String, HostPort> shardMasters) {
        ClusterMeta clusterMeta = getClusterMeta(cluster, dc);
        if (clusterMeta == null) {
            return Collections.emptySet();
        }
        String canonicalDc = resolveCanonicalDcId(dc);

        Set<MonitorShardMeta> shards = new HashSet<>();
        for (Map.Entry<String, ShardMeta> entry : clusterMeta.getShards().entrySet()) {
            String shardName = entry.getKey();
            ShardMeta shardMeta = entry.getValue();
            if (isSentinelBeaconMetaExcluded(shardMeta)) {
                continue;
            }
            List<MonitorGroupMeta> groups = shardMeta.getRedises().stream().map(redisMeta -> {
                HostPort hostPort = new HostPort(redisMeta.getIp(), redisMeta.getPort());
                MonitorGroupMeta group = new MonitorGroupMeta(hostPort.toString(), canonicalDc,
                        Collections.singleton(hostPort), redisMeta.isMaster());
                group.setAz(redisMeta.getAz());
                return group;
            }).collect(Collectors.toList());
            shards.add(new MonitorShardMeta(shardName, groups));
        }
        applyShardMasters(cluster, shards, shardMasters);
        return shards;
    }

    private void applyShardMasters(String cluster, Set<MonitorShardMeta> shards, Map<String, HostPort> shardMasters) {
        if (shardMasters == null || shardMasters.isEmpty()) {
            return;
        }

        Map<String, MonitorShardMeta> shardMap = shards.stream()
                .collect(Collectors.toMap(MonitorShardMeta::getName, shard -> shard));
        shardMasters.forEach((shardName, master) -> {
            if (master == null) {
                return;
            }
            MonitorShardMeta shard = shardMap.get(shardName);
            if (shard == null || shard.getGroups() == null) {
                logger.warn("[applyShardMasters][{}][{}] shard not found", cluster, shardName);
                return;
            }

            boolean foundMaster = shard.getGroups().stream()
                    .anyMatch(group -> group.getNodes() != null && group.getNodes().contains(master));
            if (!foundMaster) {
                logger.warn("[applyShardMasters][{}][{}] master {} not found in meta", cluster, shardName, master);
                return;
            }

            for (MonitorGroupMeta group : shard.getGroups()) {
                group.setMasterGroup(group.getNodes() != null && group.getNodes().contains(master));
            }
        });
    }

    @Override
    public boolean compareDrBeaconMetaWithXPipe(String clusterName, Set<MonitorGroupMeta> beaconGroups) {
        String activeDc = metaCache.getActiveDc(clusterName);
        if (StringUtil.isEmpty(activeDc)) {
            return false;
        }
        return compareDrBeaconMetaWithXPipe(clusterName, activeDc, beaconGroups);
    }

    @Override
    public boolean compareDrBeaconMetaWithXPipe(String clusterName, String dc, Set<MonitorGroupMeta> beaconGroups) {
        Set<MonitorGroupMeta> metaFromXPipe = buildDrBeaconGroups(clusterName, dc);
        if (metaFromXPipe.isEmpty()) {
            return false;
        }
        return metaFromXPipe.equals(beaconGroups);
    }

    private ClusterMeta getClusterMeta(String cluster, String dc) {
        DcMeta dcMeta = findDcMeta(dc);
        if (dcMeta == null) {
            return null;
        }
        return dcMeta.getClusters().get(cluster);
    }

    private DcMeta findDcMeta(String dc) {
        XpipeMeta xpipeMeta = metaCache.getXpipeMeta();
        if (xpipeMeta == null || xpipeMeta.getDcs() == null || StringUtil.isEmpty(dc)) {
            return null;
        }
        DcMeta dcMeta = xpipeMeta.getDcs().get(dc);
        if (dcMeta != null) {
            return dcMeta;
        }
        return xpipeMeta.getDcs().entrySet().stream()
                .filter(entry -> entry.getKey().equalsIgnoreCase(dc))
                .map(Map.Entry::getValue)
                .findFirst()
                .orElse(null);
    }

    private String resolveCanonicalDcId(String dc) {
        XpipeMeta xpipeMeta = metaCache.getXpipeMeta();
        if (xpipeMeta == null || xpipeMeta.getDcs() == null || StringUtil.isEmpty(dc)) {
            return dc;
        }
        if (xpipeMeta.getDcs().containsKey(dc)) {
            return dc;
        }
        return xpipeMeta.getDcs().entrySet().stream()
                .filter(entry -> entry.getKey().equalsIgnoreCase(dc))
                .map(Map.Entry::getKey)
                .findFirst()
                .orElse(dc);
    }

    private Set<String> resolveSameRegionDcs(ClusterMeta dcClusterMeta) {
        String activeDc = dcClusterMeta.getActiveDc();
        Set<String> scopeDcs = new LinkedHashSet<>();
        scopeDcs.add(activeDc);
        if (!StringUtil.isEmpty(dcClusterMeta.getBackupDcs())) {
            Arrays.stream(dcClusterMeta.getBackupDcs().split(","))
                    .map(String::trim)
                    .filter(dc -> !StringUtil.isEmpty(dc))
                    .filter(dc -> !metaCache.isCrossRegion(activeDc, dc))
                    .forEach(scopeDcs::add);
        }
        return scopeDcs;
    }

    private boolean isDcInSupportZones(String dc) {
        Set<String> supportZones = config.getBeaconSupportZones();
        if (supportZones.isEmpty()) {
            return true;
        }
        return supportZones.stream().anyMatch(zone -> metaCache.isDcInRegion(dc, zone));
    }

    private ClusterType resolveEffectiveClusterType(ClusterMeta clusterMeta) {
        ClusterType clusterType = ClusterType.lookup(clusterMeta.getType());
        if (clusterType == ClusterType.HETERO && !StringUtil.isEmpty(clusterMeta.getAzGroupType())) {
            return ClusterType.lookup(clusterMeta.getAzGroupType());
        }
        return clusterType;
    }

    @VisibleForTesting
    boolean isSentinelBeaconMetaExcluded(ShardMeta shardMeta) {
        if (shardMeta == null) {
            return false;
        }
        Long until = shardMeta.getMetaExcludeUntilTimestamp();
        return until != null && until > 0 && System.currentTimeMillis() < until;
    }

    @VisibleForTesting
    public void setMetaCache(MetaCache metaCache) {
        this.metaCache = metaCache;
    }

}
