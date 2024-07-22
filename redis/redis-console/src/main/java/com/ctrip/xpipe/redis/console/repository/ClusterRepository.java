package com.ctrip.xpipe.redis.console.repository;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.ctrip.xpipe.redis.console.entity.ClusterEntity;
import com.ctrip.xpipe.redis.console.mapper.ClusterMapper;
import org.springframework.stereotype.Repository;

import javax.annotation.Resource;
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

    public List<ClusterEntity> selectAllByIds(List<Long> ids) {
        return clusterMapper.selectBatchIds(ids);
    }

}
