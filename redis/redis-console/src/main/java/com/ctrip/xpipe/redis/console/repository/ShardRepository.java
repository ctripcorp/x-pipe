package com.ctrip.xpipe.redis.console.repository;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.ctrip.xpipe.redis.console.entity.ShardEntity;
import com.ctrip.xpipe.redis.console.mapper.ShardMapper;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;

@Repository
public class ShardRepository {

    @Resource
    private ShardMapper shardMapper;

    public List<ShardEntity> selectByClusterId(Long clusterId) {
        if (clusterId == null) {
            return Collections.emptyList();
        }
        QueryWrapper<ShardEntity> wrapper = new QueryWrapper<>();
        wrapper.eq(ShardEntity.CLUSTER_ID, clusterId);
        return shardMapper.selectList(wrapper);
    }

    public List<ShardEntity> selectByAzGroupClusterId(Long azGroupClusterId) {
        if (azGroupClusterId == null) {
            return Collections.emptyList();
        }
        QueryWrapper<ShardEntity> wrapper = new QueryWrapper<>();
        wrapper.eq(ShardEntity.AZ_GROUP_CLUSTER_ID, azGroupClusterId);
        return shardMapper.selectList(wrapper);
    }

    public long insertAndGetId(String shardName, Long clusterId, Long azGroupClusterId) {
        if (shardName == null || clusterId == null || azGroupClusterId == null) {
            return 0L;
        }
        ShardEntity shard = new ShardEntity();
        shard.setShardName(shardName);
        shard.setClusterId(clusterId);
        shard.setAzGroupClusterId(azGroupClusterId);
        shard.setSetinelMonitorName(shardName);
        int insert = shardMapper.insert(shard);
        if (insert == 1) {
            return shard.getId();
        } else {
            return 0L;
        }
    }

    public void updateClusterId(Long id, Long clusterId) {
        if (id == null || clusterId == null) {
            return;
        }
        UpdateWrapper<ShardEntity> wrapper = new UpdateWrapper<>();
        wrapper.set(ShardEntity.CLUSTER_ID, clusterId).eq(ShardEntity.ID, id);
        shardMapper.update(null, wrapper);
    }

    public void batchUpdateAzGroupClusterId(List<Long> ids, Long azGroupClusterId) {
        if (CollectionUtils.isEmpty(ids) || azGroupClusterId == null) {
            return;
        }
        UpdateWrapper<ShardEntity> wrapper = new UpdateWrapper<>();
        wrapper.set(ShardEntity.AZ_GROUP_CLUSTER_ID, azGroupClusterId).in(ShardEntity.ID, ids);
        shardMapper.update(null, wrapper);
    }

}
