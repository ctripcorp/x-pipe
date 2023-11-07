package com.ctrip.xpipe.redis.console.repository;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.ctrip.xpipe.redis.console.entity.DcClusterEntity;
import com.ctrip.xpipe.redis.console.mapper.DcClusterMapper;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Repository
public class DcClusterRepository {

    @Resource
    private DcClusterMapper dcClusterMapper;

    public void insert(DcClusterEntity dcCluster) {
        dcClusterMapper.insert(dcCluster);
    }

    public List<DcClusterEntity> selectByClusterId(Long clusterId) {
        if (clusterId == null) {
            return Collections.emptyList();
        }
        QueryWrapper<DcClusterEntity> wrapper = new QueryWrapper<>();
        wrapper.eq(DcClusterEntity.CLUSTER_ID, clusterId);
        return dcClusterMapper.selectList(wrapper);
    }

    public List<DcClusterEntity> selectByClusterIds(List<Long> clusterIds) {
        if (CollectionUtils.isEmpty(clusterIds)) {
            return Collections.emptyList();
        }
        QueryWrapper<DcClusterEntity> wrapper = new QueryWrapper<>();
        wrapper.in(DcClusterEntity.CLUSTER_ID, clusterIds);
        return dcClusterMapper.selectList(wrapper);
    }

    public DcClusterEntity selectByClusterIdAndDcId(Long clusterId, Long dcId) {
        if (clusterId == null || dcId == null) {
            return null;
        }
        QueryWrapper<DcClusterEntity> wrapper = new QueryWrapper<>();
        wrapper.eq(DcClusterEntity.CLUSTER_ID, clusterId).eq(DcClusterEntity.DC_ID, dcId);
        return dcClusterMapper.selectOne(wrapper);
    }

    public List<Long> selectIdByAzGroupClusterId(Long azGroupClusterId) {
        if (azGroupClusterId == null) {
            return Collections.emptyList();
        }
        QueryWrapper<DcClusterEntity> wrapper = new QueryWrapper<>();
        wrapper.select(DcClusterEntity.DC_CLUSTER_ID);
        wrapper.eq(DcClusterEntity.AZ_GROUP_CLUSTER_ID, azGroupClusterId);
        List<DcClusterEntity> dcClusters = dcClusterMapper.selectList(wrapper);
        return dcClusters.stream().map(DcClusterEntity::getDcClusterId).collect(Collectors.toList());
    }

    public List<DcClusterEntity> selectByAzGroupClusterId(Long azGroupClusterId) {
        if (azGroupClusterId == null) {
            return Collections.emptyList();
        }
        QueryWrapper<DcClusterEntity> wrapper = new QueryWrapper<>();
        wrapper.eq(DcClusterEntity.AZ_GROUP_CLUSTER_ID, azGroupClusterId);
        return dcClusterMapper.selectList(wrapper);
    }

    public void batchUpdateClusterId(List<Long> dcClusterIds, Long clusterId) {
        if (CollectionUtils.isEmpty(dcClusterIds) || clusterId == null) {
            return;
        }
        UpdateWrapper<DcClusterEntity> wrapper = new UpdateWrapper<>();
        wrapper.set(DcClusterEntity.CLUSTER_ID, clusterId);
        wrapper.in(DcClusterEntity.DC_CLUSTER_ID, dcClusterIds);
        dcClusterMapper.update(null, wrapper);
    }

    public void updateAzGroupClusterId(Long dcClusterId, Long azGroupClusterId) {
        if (dcClusterId == null || azGroupClusterId == null) {
            return;
        }
        UpdateWrapper<DcClusterEntity> wrapper = new UpdateWrapper<>();
        wrapper.set(DcClusterEntity.AZ_GROUP_CLUSTER_ID, azGroupClusterId);
        wrapper.eq(DcClusterEntity.DC_CLUSTER_ID, dcClusterId);
        dcClusterMapper.update(null, wrapper);
    }

    public void batchUpdateAzGroupClusterId(List<Long> dcClusterIds, Long azGroupClusterId) {
        if (CollectionUtils.isEmpty(dcClusterIds) || azGroupClusterId == null) {
            return;
        }
        UpdateWrapper<DcClusterEntity> wrapper = new UpdateWrapper<>();
        wrapper.set(DcClusterEntity.AZ_GROUP_CLUSTER_ID, azGroupClusterId);
        wrapper.in(DcClusterEntity.DC_CLUSTER_ID, dcClusterIds);
        dcClusterMapper.update(null, wrapper);
    }

    public void deleteByClusterIdAndDcId(Long clusterId, Long azId) {
        if (clusterId == null || azId == null) {
            return;
        }
        QueryWrapper<DcClusterEntity> wrapper = new QueryWrapper<>();
        wrapper.eq(DcClusterEntity.CLUSTER_ID, clusterId).eq(DcClusterEntity.DC_ID, azId);
        dcClusterMapper.delete(wrapper);
    }
}
