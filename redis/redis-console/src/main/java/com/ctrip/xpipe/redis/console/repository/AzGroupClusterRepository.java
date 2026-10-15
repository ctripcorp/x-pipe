package com.ctrip.xpipe.redis.console.repository;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.entity.AzGroupClusterEntity;

import java.util.List;

public interface AzGroupClusterRepository {

    long countByClusterId(Long clusterId);

    List<AzGroupClusterEntity> selectAll();

    AzGroupClusterEntity selectById(Long id);

    ClusterType selectAzGroupTypeById(Long id);

    List<AzGroupClusterEntity> selectByClusterId(Long clusterId);

    List<AzGroupClusterEntity> selectByClusterIds(List<Long> clusterIds);

    List<AzGroupClusterEntity> selectByAzGroupIds(List<Long> azGroupIds);

    AzGroupClusterEntity selectByClusterIdAndAzGroupId(Long clusterId, Long azGroupId);

    List<AzGroupClusterEntity> selectByAzGroupClusterType(String azGroupClusterType);

    AzGroupClusterEntity selectByClusterIdAndAz(Long clusterId, String az);

    void insert(AzGroupClusterEntity azGroupCluster);

    void batchUpdate(List<AzGroupClusterEntity> azGroupClusters);

    void updateClusterId(Long id, Long clusterId);

    void updateAzGroupId(Long id, Long azGroupId);

    void updateActiveAzId(Long id, Long activeAzId);

    void deleteByClusterId(Long clusterId);

    void deleteByClusterIdAndAzGroupId(Long clusterId, Long azGroupId);
}
