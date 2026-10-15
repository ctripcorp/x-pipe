package com.ctrip.xpipe.redis.console.repository;

import com.ctrip.xpipe.cache.TimeBoundCache;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.spring.ConsoleDisableDbCondition;
import com.ctrip.xpipe.redis.checker.spring.DisableDbMode;
import com.ctrip.xpipe.redis.console.cache.AzGroupCache;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.entity.AzGroupClusterEntity;
import com.ctrip.xpipe.redis.console.model.AzGroupModel;
import com.ctrip.xpipe.redis.console.resources.ConsolePortalService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Conditional;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.stream.Collectors;

@Repository("azGroupClusterRepository")
@Conditional(ConsoleDisableDbCondition.class)
@DisableDbMode(true)
public class AzGroupClusterRepositoryWithoutDB implements AzGroupClusterRepository {

    private final ConsolePortalService consolePortalService;
    private final ConsoleConfig consoleConfig;
    private final AzGroupCache azGroupCache;

    private TimeBoundCache<List<AzGroupClusterEntity>> allAzGroupClusters;

    @Autowired
    public AzGroupClusterRepositoryWithoutDB(ConsolePortalService consolePortalService, ConsoleConfig consoleConfig,
                                             AzGroupCache azGroupCache) {
        this.consolePortalService = consolePortalService;
        this.consoleConfig = consoleConfig;
        this.azGroupCache = azGroupCache;
    }

    @PostConstruct
    public void init() {
        allAzGroupClusters = new TimeBoundCache<>(consoleConfig::getCacheRefreshInterval,
                consolePortalService::getAllAzGroupClusters);
    }

    @Override
    public long countByClusterId(Long clusterId) {
        return selectByClusterId(clusterId).size();
    }

    @Override
    public List<AzGroupClusterEntity> selectAll() {
        List<AzGroupClusterEntity> result = allAzGroupClusters.getData();
        return result == null ? Collections.emptyList() : result;
    }

    @Override
    public AzGroupClusterEntity selectById(Long id) {
        if (id == null || id <= 0L) {
            return null;
        }
        return selectAll().stream()
                .filter(entity -> Objects.equals(id, entity.getId()))
                .findFirst()
                .orElse(null);
    }

    @Override
    public ClusterType selectAzGroupTypeById(Long id) {
        AzGroupClusterEntity entity = selectById(id);
        if (entity == null || StringUtils.isEmpty(entity.getAzGroupClusterType())) {
            return null;
        }
        return ClusterType.lookup(entity.getAzGroupClusterType());
    }

    @Override
    public List<AzGroupClusterEntity> selectByClusterId(Long clusterId) {
        if (clusterId == null) {
            return Collections.emptyList();
        }
        return selectAll().stream()
                .filter(entity -> Objects.equals(clusterId, entity.getClusterId()))
                .collect(Collectors.toList());
    }

    @Override
    public List<AzGroupClusterEntity> selectByClusterIds(List<Long> clusterIds) {
        if (CollectionUtils.isEmpty(clusterIds)) {
            return Collections.emptyList();
        }
        Set<Long> clusterIdSet = new HashSet<>(clusterIds);
        return selectAll().stream()
                .filter(entity -> clusterIdSet.contains(entity.getClusterId()))
                .collect(Collectors.toList());
    }

    @Override
    public List<AzGroupClusterEntity> selectByAzGroupIds(List<Long> azGroupIds) {
        if (CollectionUtils.isEmpty(azGroupIds)) {
            return Collections.emptyList();
        }
        Set<Long> azGroupIdSet = new HashSet<>(azGroupIds);
        return selectAll().stream()
                .filter(entity -> azGroupIdSet.contains(entity.getAzGroupId()))
                .collect(Collectors.toList());
    }

    @Override
    public AzGroupClusterEntity selectByClusterIdAndAzGroupId(Long clusterId, Long azGroupId) {
        if (clusterId == null || azGroupId == null) {
            return null;
        }
        return selectAll().stream()
                .filter(entity -> Objects.equals(clusterId, entity.getClusterId()))
                .filter(entity -> Objects.equals(azGroupId, entity.getAzGroupId()))
                .findFirst()
                .orElse(null);
    }

    @Override
    public List<AzGroupClusterEntity> selectByAzGroupClusterType(String azGroupClusterType) {
        if (azGroupClusterType == null) {
            return Collections.emptyList();
        }
        return selectAll().stream()
                .filter(entity -> azGroupClusterType.equals(entity.getAzGroupClusterType()))
                .collect(Collectors.toList());
    }

    @Override
    public AzGroupClusterEntity selectByClusterIdAndAz(Long clusterId, String az) {
        if (clusterId == null || az == null) {
            return null;
        }
        for (AzGroupClusterEntity entity : selectByClusterId(clusterId)) {
            AzGroupModel azGroup = azGroupCache.getAzGroupById(entity.getAzGroupId());
            if (azGroup != null && azGroup.containsAz(az)) {
                return entity;
            }
        }
        return null;
    }

    @Override
    public void insert(AzGroupClusterEntity azGroupCluster) {
        throw readOnlyException();
    }

    @Override
    public void batchUpdate(List<AzGroupClusterEntity> azGroupClusters) {
        throw readOnlyException();
    }

    @Override
    public void updateClusterId(Long id, Long clusterId) {
        throw readOnlyException();
    }

    @Override
    public void updateAzGroupId(Long id, Long azGroupId) {
        throw readOnlyException();
    }

    @Override
    public void updateActiveAzId(Long id, Long activeAzId) {
        throw readOnlyException();
    }

    @Override
    public void deleteByClusterId(Long clusterId) {
        throw readOnlyException();
    }

    @Override
    public void deleteByClusterIdAndAzGroupId(Long clusterId, Long azGroupId) {
        throw readOnlyException();
    }

    private UnsupportedOperationException readOnlyException() {
        return new UnsupportedOperationException("AzGroupClusterRepository is read-only when database is disabled");
    }
}
