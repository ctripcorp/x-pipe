package com.ctrip.xpipe.redis.console.repository;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.ctrip.xpipe.redis.console.entity.ClusterEntity;
import com.ctrip.xpipe.redis.console.mapper.ClusterMapper;
import org.springframework.stereotype.Repository;

import javax.annotation.Resource;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

@Repository
public class ClusterRepository {

    @Resource
    private ClusterMapper clusterMapper;

    public ClusterEntity selectByClusterName(String clusterName) {
        if (clusterName == null) {
            return null;
        }
        QueryWrapper<ClusterEntity> wrapper = new QueryWrapper<>();
        wrapper.eq(ClusterEntity.CLUSTER_NAME, clusterName);
        return clusterMapper.selectOne(wrapper);
    }

    public List<ClusterEntity> selectAllByClusterName(Collection<String> clusterNames) {
        if (clusterNames == null || clusterNames.isEmpty()) {
            return Collections.emptyList();
        }
        QueryWrapper<ClusterEntity> query = new QueryWrapper<>();
        query.in(ClusterEntity.CLUSTER_NAME, clusterNames);
        return clusterMapper.selectList(query);
    }

    public List<ClusterEntity> selectAllByIds(List<Long> ids) {
        return clusterMapper.selectBatchIds(ids);
    }

}
