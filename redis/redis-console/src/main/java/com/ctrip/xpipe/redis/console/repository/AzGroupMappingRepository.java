package com.ctrip.xpipe.redis.console.repository;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.ctrip.xpipe.redis.console.entity.AzGroupMappingEntity;
import com.ctrip.xpipe.redis.console.mapper.AzGroupMappingMapper;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;
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

    /**
     * Insert mappings for one az group. Az count is small; use the Spring-managed mapper
     * so writes join {@code @Transactional} and are visible to a following cache reload.
     */
    public void batchInsert(Long azGroupId, List<Long> azIds) {
        if (azGroupId == null || CollectionUtils.isEmpty(azIds)) {
            return;
        }
        for (Long azId : azIds) {
            azGroupMappingMapper.insert(new AzGroupMappingEntity().setAzGroupId(azGroupId).setAzId(azId));
        }
    }

    public void deleteByAzGroupId(Long azGroupId) {
        if (azGroupId == null) {
            return;
        }
        QueryWrapper<AzGroupMappingEntity> wrapper = new QueryWrapper<>();
        wrapper.eq(AzGroupMappingEntity.AZ_GROUP_ID, azGroupId);
        azGroupMappingMapper.delete(wrapper);
    }
}
