package com.ctrip.xpipe.redis.console.repository;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.ctrip.xpipe.redis.console.entity.ShardEntity;
import com.ctrip.xpipe.redis.console.mapper.ShardMapper;
import jakarta.annotation.Resource;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

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

    public List<Long> selectIdByAzGroupClusterId(Long azGroupClusterId) {
        if (azGroupClusterId == null) {
            return Collections.emptyList();
        }
        QueryWrapper<ShardEntity> wrapper = new QueryWrapper<>();
        wrapper.select(ShardEntity.ID);
        wrapper.eq(ShardEntity.AZ_GROUP_CLUSTER_ID, azGroupClusterId);
        List<ShardEntity> shards = shardMapper.selectList(wrapper);
        return shards.stream().map(ShardEntity::getId).collect(Collectors.toList());
    }

    public List<ShardEntity> selectByAzGroupClusterId(Long azGroupClusterId) {
        if (azGroupClusterId == null) {
            return Collections.emptyList();
        }
        QueryWrapper<ShardEntity> wrapper = new QueryWrapper<>();
        wrapper.eq(ShardEntity.AZ_GROUP_CLUSTER_ID, azGroupClusterId);
        return shardMapper.selectList(wrapper);
    }

    public void insert(ShardEntity shard) {
        if (shard == null) {
            return;
        }
        shardMapper.insert(shard);
    }

    public void batchUpdateClusterId(List<Long> ids, Long clusterId) {
        if (CollectionUtils.isEmpty(ids) || clusterId == null) {
            return;
        }
        UpdateWrapper<ShardEntity> wrapper = new UpdateWrapper<>();
        wrapper.set(ShardEntity.CLUSTER_ID, clusterId);
        wrapper.in(ShardEntity.ID, ids);
        shardMapper.update(null, wrapper);
    }

    public void batchUpdateAzGroupClusterId(List<Long> ids, Long azGroupClusterId) {
        if (CollectionUtils.isEmpty(ids) || azGroupClusterId == null) {
            return;
        }
        UpdateWrapper<ShardEntity> wrapper = new UpdateWrapper<>();
        wrapper.set(ShardEntity.AZ_GROUP_CLUSTER_ID, azGroupClusterId);
        wrapper.in(ShardEntity.ID, ids);
        shardMapper.update(null, wrapper);
    }

}
