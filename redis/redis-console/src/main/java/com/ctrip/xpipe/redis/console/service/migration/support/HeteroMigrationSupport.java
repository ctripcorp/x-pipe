package com.ctrip.xpipe.redis.console.service.migration.support;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.cache.AzGroupCache;
import com.ctrip.xpipe.redis.console.cache.DcCache;
import com.ctrip.xpipe.redis.console.controller.api.migrate.meta.BeaconMigrationRequest;
import com.ctrip.xpipe.redis.console.entity.AzGroupClusterEntity;
import com.ctrip.xpipe.redis.console.model.AzGroupModel;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.repository.AzGroupClusterRepository;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Component
public class HeteroMigrationSupport {

    public static final String DEFAULT_PREFER_REGION = "SHA";

    @Resource
    private AzGroupClusterRepository azGroupClusterRepository;

    @Resource
    private AzGroupCache azGroupCache;

    public boolean isHeteroCluster(ClusterTbl cluster) {
        return cluster != null
                && ClusterType.isSameClusterType(cluster.getClusterType(), ClusterType.HETERO);
    }

    public List<AzGroupClusterEntity> listOneWayAzGroupClustersSorted(long clusterId) {
        if (clusterId <= 0) {
            return Collections.emptyList();
        }
        return listOneWayAzGroupClustersSorted(Collections.singletonList(clusterId))
                .getOrDefault(clusterId, Collections.emptyList());
    }

    public Map<Long, List<AzGroupClusterEntity>> listOneWayAzGroupClustersSorted(Collection<Long> clusterIds) {
        return listAzGroupClustersSortedByType(clusterIds, ClusterType.ONE_WAY);
    }

    /**
     * Az group clusters shown on cluster_list (ONE_WAY + SINGLE_DC), each with its own active_az.
     */
    public Map<Long, List<AzGroupClusterEntity>> listDisplayAzGroupClustersSorted(Collection<Long> clusterIds) {
        return listAzGroupClustersSortedByType(clusterIds, ClusterType.ONE_WAY, ClusterType.SINGLE_DC);
    }

    private Map<Long, List<AzGroupClusterEntity>> listAzGroupClustersSortedByType(Collection<Long> clusterIds,
                                                                                  ClusterType... types) {
        if (CollectionUtils.isEmpty(clusterIds) || types == null || types.length == 0) {
            return Collections.emptyMap();
        }
        Set<ClusterType> allowedTypes = new HashSet<>();
        for (ClusterType type : types) {
            allowedTypes.add(type);
        }
        List<Long> ids = clusterIds.stream().filter(id -> id != null && id > 0).distinct().collect(Collectors.toList());
        if (ids.isEmpty()) {
            return Collections.emptyMap();
        }
        List<AzGroupClusterEntity> azGroupClusters = azGroupClusterRepository.selectByClusterIds(ids);
        if (CollectionUtils.isEmpty(azGroupClusters)) {
            return Collections.emptyMap();
        }
        Map<Long, List<AzGroupClusterEntity>> result = new HashMap<>();
        for (AzGroupClusterEntity azGroupCluster : azGroupClusters) {
            ClusterType azGroupType = ClusterType.lookup(azGroupCluster.getAzGroupClusterType());
            if (!allowedTypes.contains(azGroupType)) {
                continue;
            }
            result.computeIfAbsent(azGroupCluster.getClusterId(), ignored -> new ArrayList<>()).add(azGroupCluster);
        }
        for (List<AzGroupClusterEntity> azGroupList : result.values()) {
            sortAzGroupClustersByRegion(azGroupList);
        }
        return result;
    }

    public AzGroupClusterEntity pickFirstOneWayAzGroupCluster(long clusterId) {
        List<AzGroupClusterEntity> sorted = listOneWayAzGroupClustersSorted(clusterId);
        return sorted.isEmpty() ? null : sorted.get(0);
    }

    public AzGroupClusterEntity pickOneWayAzGroupClusterByRegion(long clusterId, String preferRegion) {
        return pickOneWayAzGroupClusterByRegion(listOneWayAzGroupClustersSorted(clusterId), preferRegion);
    }

    public AzGroupClusterEntity pickOneWayAzGroupClusterByRegion(List<AzGroupClusterEntity> sortedOneWay,
                                                                 String preferRegion) {
        if (CollectionUtils.isEmpty(sortedOneWay)) {
            return null;
        }
        if (!StringUtils.isEmpty(preferRegion)) {
            for (AzGroupClusterEntity azGroupCluster : sortedOneWay) {
                AzGroupModel azGroup = azGroupCache.getAzGroupById(azGroupCluster.getAzGroupId());
                if (azGroup != null && preferRegion.equalsIgnoreCase(azGroup.getRegion())) {
                    return azGroupCluster;
                }
            }
        }
        return sortedOneWay.get(0);
    }

    public void sortAzGroupClustersByRegion(List<AzGroupClusterEntity> azGroupClusters) {
        if (CollectionUtils.isEmpty(azGroupClusters)) {
            return;
        }
        Map<Long, String> regionByAzGroupId = buildAzGroupIdToRegionMap(azGroupClusters);
        azGroupClusters.sort(azGroupClusterOrder(regionByAzGroupId));
    }

    private Map<Long, String> buildAzGroupIdToRegionMap(Collection<AzGroupClusterEntity> azGroupClusters) {
        Map<Long, String> regionByAzGroupId = new HashMap<>();
        for (AzGroupClusterEntity azGroupCluster : azGroupClusters) {
            long azGroupId = azGroupCluster.getAzGroupId();
            regionByAzGroupId.computeIfAbsent(azGroupId, id -> {
                AzGroupModel azGroup = azGroupCache.getAzGroupById(id);
                return azGroup != null ? azGroup.getRegion() : "";
            });
        }
        return regionByAzGroupId;
    }

    private Comparator<AzGroupClusterEntity> azGroupClusterOrder(Map<Long, String> regionByAzGroupId) {
        return Comparator
                .comparing((AzGroupClusterEntity azGroupCluster) ->
                        regionByAzGroupId.getOrDefault(azGroupCluster.getAzGroupId(), ""))
                .thenComparing(AzGroupClusterEntity::getId);
    }

    public AzGroupClusterEntity resolveAzGroupCluster(long clusterId, String dcName) {
        if (clusterId <= 0 || StringUtils.isEmpty(dcName)) {
            return null;
        }
        return resolveAzGroupClusters(Collections.singletonList(clusterId), dcName).get(clusterId);
    }

    public Map<Long, AzGroupClusterEntity> resolveAzGroupClusters(Collection<Long> clusterIds, String dcName) {
        return resolveAzGroupClusters(clusterIds, dcName, false);
    }

    public Map<Long, AzGroupClusterEntity> resolveMigrationAzGroupClusters(Collection<Long> clusterIds, String dcName) {
        return resolveAzGroupClusters(clusterIds, dcName, true);
    }

    private Map<Long, AzGroupClusterEntity> resolveAzGroupClusters(Collection<Long> clusterIds, String dcName,
                                                                   boolean migrationOnly) {
        if (CollectionUtils.isEmpty(clusterIds) || StringUtils.isEmpty(dcName)) {
            return Collections.emptyMap();
        }
        List<AzGroupClusterEntity> azGroupClusters =
                azGroupClusterRepository.selectByClusterIds(new ArrayList<>(clusterIds));
        if (CollectionUtils.isEmpty(azGroupClusters)) {
            return Collections.emptyMap();
        }
        Map<Long, AzGroupClusterEntity> result = new HashMap<>();
        for (AzGroupClusterEntity azGroupCluster : azGroupClusters) {
            if (migrationOnly && !ClusterType.isSameClusterType(
                    azGroupCluster.getAzGroupClusterType(), ClusterType.ONE_WAY)) {
                continue;
            }
            Long clusterId = azGroupCluster.getClusterId();
            AzGroupModel azGroup = azGroupCache.getAzGroupById(azGroupCluster.getAzGroupId());
            if (azGroup == null || !azGroup.containsAz(dcName)) {
                continue;
            }
            AzGroupClusterEntity existing = result.get(clusterId);
            if (existing == null || preferAzGroupClusterForMigration(azGroupCluster, existing)) {
                result.put(clusterId, azGroupCluster);
            }
        }
        return result;
    }

    private boolean preferAzGroupClusterForMigration(AzGroupClusterEntity candidate, AzGroupClusterEntity current) {
        boolean candidateOneWay = ClusterType.isSameClusterType(candidate.getAzGroupClusterType(), ClusterType.ONE_WAY);
        boolean currentOneWay = ClusterType.isSameClusterType(current.getAzGroupClusterType(), ClusterType.ONE_WAY);
        return candidateOneWay && !currentOneWay;
    }

    public AzGroupClusterEntity resolveAzGroupClusterForBeaconRequest(ClusterTbl cluster,
                                                                      BeaconMigrationRequest request) {
        if (!isHeteroCluster(cluster)) {
            return null;
        }
        String referenceDc = resolveReferenceDc(request);
        if (referenceDc == null) {
            return null;
        }
        return resolveAzGroupCluster(cluster.getId(), referenceDc);
    }

    public Set<String> filterDcsInSameAzGroup(AzGroupClusterEntity azGroupCluster, Set<String> dcs) {
        if (azGroupCluster == null || CollectionUtils.isEmpty(dcs)) {
            return dcs;
        }
        AzGroupModel azGroup = azGroupCache.getAzGroupById(azGroupCluster.getAzGroupId());
        if (azGroup == null) {
            return dcs;
        }
        Set<String> azGroupDcs = new HashSet<>(azGroup.getAzsAsList());
        Set<String> filtered = new HashSet<>(dcs);
        filtered.retainAll(azGroupDcs);
        return filtered;
    }

    public boolean isSameAzGroup(long clusterId, String sourceDcName, String targetDcName) {
        AzGroupClusterEntity sourceAzGroupCluster = resolveAzGroupCluster(clusterId, sourceDcName);
        AzGroupClusterEntity targetAzGroupCluster = resolveAzGroupCluster(clusterId, targetDcName);
        if (sourceAzGroupCluster == null || targetAzGroupCluster == null) {
            return false;
        }
        return sourceAzGroupCluster.getId().equals(targetAzGroupCluster.getId());
    }

    public Long resolveMigrationAzGroupClusterId(long clusterId, String sourceDcName) {
        AzGroupClusterEntity azGroupCluster = resolveMigrationAzGroupClusters(
                Collections.singletonList(clusterId), sourceDcName).get(clusterId);
        return azGroupCluster == null ? null : azGroupCluster.getId();
    }

    public Long resolveAzGroupClusterIdBySourceDc(long clusterId, long sourceDcId, DcCache dcCache) {
        DcTbl sourceDc = dcCache.find(sourceDcId);
        if (sourceDc == null) {
            return null;
        }
        return resolveMigrationAzGroupClusterId(clusterId, sourceDc.getDcName());
    }

    public Set<String> getAzGroupDcNames(AzGroupClusterEntity azGroupCluster) {
        if (azGroupCluster == null) {
            return Collections.emptySet();
        }
        AzGroupModel azGroup = azGroupCache.getAzGroupById(azGroupCluster.getAzGroupId());
        if (azGroup == null) {
            return Collections.emptySet();
        }
        return new HashSet<>(azGroup.getAzsAsList());
    }

    private String resolveReferenceDc(BeaconMigrationRequest request) {
        if (!CollectionUtils.isEmpty(request.getFailDcs())) {
            return request.getFailDcs().iterator().next();
        }
        if (request.getIsForced() && !StringUtils.isEmpty(request.getTargetIDC())) {
            return request.getTargetIDC();
        }
        return null;
    }
}
