package com.ctrip.xpipe.redis.console.repository;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.ctrip.xpipe.redis.console.entity.AzGroupMappingEntity;
import com.ctrip.xpipe.redis.console.mapper.AzGroupMappingMapper;
import jakarta.annotation.Resource;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Repository
public class AzGroupMappingRepository {

    @Resource
    private AzGroupMappingMapper azGroupMappingMapper;

    public Map<Long, List<Long>> getAzGroupAzsMap() {
        QueryWrapper<AzGroupMappingEntity> wrapper = new QueryWrapper<>();
        wrapper.select(AzGroupMappingEntity.AZ_ID, AzGroupMappingEntity.AZ_GROUP_ID);
        List<AzGroupMappingEntity> entities = azGroupMappingMapper.selectList(wrapper);
        return entities.stream().collect(Collectors.groupingBy(AzGroupMappingEntity::getAzGroupId,
            Collectors.mapping(AzGroupMappingEntity::getAzId, Collectors.toList())));
    }
}
