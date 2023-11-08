package com.ctrip.xpipe.redis.console.repository;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.cache.AzGroupCache;
import com.ctrip.xpipe.redis.console.entity.AzGroupClusterEntity;
import com.ctrip.xpipe.redis.console.mapper.AzGroupClusterMapper;
import com.ctrip.xpipe.redis.console.model.AzGroupModel;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;

@Repository
public class AzGroupClusterRepository {

    @Resource
    private AzGroupCache azGroupCache;
    @Resource
    private AzGroupClusterMapper azGroupClusterMapper;

    public long countByClusterId(Long clusterId) {
        if (clusterId == null) {
            return 0;
        }
        QueryWrapper<AzGroupClusterEntity> wrapper = new QueryWrapper<>();
        wrapper.eq(AzGroupClusterEntity.CLUSTER_ID, clusterId);
        return azGroupClusterMapper.selectCount(wrapper);
    }

    public List<AzGroupClusterEntity> selectAll() {
        return azGroupClusterMapper.selectList(null);
    }

    public AzGroupClusterEntity selectById(Long id) {
        if (id == null || id <= 0L) {
            return null;
        }
        return azGroupClusterMapper.selectById(id);
    }

    public ClusterType selectAzGroupTypeById(Long id) {
        if (id == null || id <= 0L) {
            return null;
        }
        QueryWrapper<AzGroupClusterEntity> wrapper = new QueryWrapper<>();
        wrapper.select(AzGroupClusterEntity.AZ_GROUP_CLUSTER_TYPE).eq(AzGroupClusterEntity.ID, id);
        AzGroupClusterEntity azGroupCluster = azGroupClusterMapper.selectOne(wrapper);
        if (azGroupCluster == null) {
            return null;
        }
        String azGroupType = azGroupCluster.getAzGroupClusterType();
        return StringUtils.isEmpty(azGroupType) ? null : ClusterType.lookup(azGroupType);
    }

    public List<AzGroupClusterEntity> selectByClusterId(Long clusterId) {
        if (clusterId == null) {
            return Collections.emptyList();
        }
        QueryWrapper<AzGroupClusterEntity> wrapper = new QueryWrapper<>();
        wrapper.eq(AzGroupClusterEntity.CLUSTER_ID, clusterId);
        return azGroupClusterMapper.selectList(wrapper);
    }

    public List<AzGroupClusterEntity> selectByClusterIds(List<Long> clusterIds) {
        if (CollectionUtils.isEmpty(clusterIds)) {
            return Collections.emptyList();
        }
        QueryWrapper<AzGroupClusterEntity> wrapper = new QueryWrapper<>();
        wrapper.in(AzGroupClusterEntity.CLUSTER_ID, clusterIds);
        return azGroupClusterMapper.selectList(wrapper);
    }

    public AzGroupClusterEntity selectByClusterIdAndAzGroupId(Long clusterId, Long azGroupId) {
        if (clusterId == null || azGroupId == null) {
            return null;
        }
        QueryWrapper<AzGroupClusterEntity> wrapper = new QueryWrapper<>();
        wrapper.eq(AzGroupClusterEntity.CLUSTER_ID, clusterId).eq(AzGroupClusterEntity.AZ_GROUP_ID, azGroupId);
        return azGroupClusterMapper.selectOne(wrapper);
    }

    public List<AzGroupClusterEntity> selectByAzGroupClusterType(String azGroupClusterType) {
        if (azGroupClusterType == null) {
            return Collections.emptyList();
        }
        QueryWrapper<AzGroupClusterEntity> wrapper = new QueryWrapper<>();
        wrapper.eq(AzGroupClusterEntity.AZ_GROUP_CLUSTER_TYPE, azGroupClusterType);
        return azGroupClusterMapper.selectList(wrapper);
    }

    public AzGroupClusterEntity selectByClusterIdAndAz(Long clusterId, String az) {
        if (clusterId == null || az == null) {
            return null;
        }
        QueryWrapper<AzGroupClusterEntity> wrapper = new QueryWrapper<>();
        wrapper.eq(AzGroupClusterEntity.CLUSTER_ID, clusterId);
        List<AzGroupClusterEntity> azGroupClusters = azGroupClusterMapper.selectList(wrapper);
        if (CollectionUtils.isEmpty(azGroupClusters)) {
            return null;
        }
        for (AzGroupClusterEntity azGroupCluster : azGroupClusters) {
            AzGroupModel azGroup = azGroupCache.getAzGroupById(azGroupCluster.getAzGroupId());
            if (azGroup != null && azGroup.containsAz(az)) {
                return azGroupCluster;
            }
        }
        return null;
    }


    public void insert(AzGroupClusterEntity azGroupCluster) {
        azGroupClusterMapper.insert(azGroupCluster);
    }

    public void updateClusterId(Long id, Long clusterId) {
        if (id == null || clusterId == null) {
            return;
        }
        UpdateWrapper<AzGroupClusterEntity> wrapper = new UpdateWrapper<>();
        wrapper.set(AzGroupClusterEntity.CLUSTER_ID, clusterId).eq(AzGroupClusterEntity.ID, id);
        azGroupClusterMapper.update(null, wrapper);
    }

    public void updateAzGroupId(Long id, Long azGroupId) {
        if (id == null || azGroupId == null) {
            return;
        }
        UpdateWrapper<AzGroupClusterEntity> wrapper = new UpdateWrapper<>();
        wrapper.set(AzGroupClusterEntity.AZ_GROUP_ID, azGroupId).eq(AzGroupClusterEntity.ID, id);
        azGroupClusterMapper.update(null, wrapper);
    }

    public void updateActiveAzId(Long id, Long activeAzId) {
        if (id == null || activeAzId == null) {
            return;
        }
        UpdateWrapper<AzGroupClusterEntity> wrapper = new UpdateWrapper<>();
        wrapper.set(AzGroupClusterEntity.ACTIVE_AZ_ID, activeAzId).eq(AzGroupClusterEntity.ID, id);
        azGroupClusterMapper.update(null, wrapper);
    }

    public void deleteByClusterId(Long clusterId) {
        if (clusterId == null) {
            return;
        }
        QueryWrapper<AzGroupClusterEntity> wrapper = new QueryWrapper<>();
        wrapper.eq(AzGroupClusterEntity.CLUSTER_ID, clusterId);
        azGroupClusterMapper.delete(wrapper);
    }

    public void deleteByClusterIdAndAzGroupId(Long clusterId, Long azGroupId) {
        if (clusterId == null || azGroupId == null) {
            return;
        }
        QueryWrapper<AzGroupClusterEntity> wrapper = new QueryWrapper<>();
        wrapper.eq(AzGroupClusterEntity.CLUSTER_ID, clusterId).eq(AzGroupClusterEntity.AZ_GROUP_ID, azGroupId);
        azGroupClusterMapper.delete(wrapper);
    }
}
