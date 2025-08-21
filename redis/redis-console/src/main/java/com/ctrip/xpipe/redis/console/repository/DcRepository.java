package com.ctrip.xpipe.redis.console.repository;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.ctrip.xpipe.redis.console.entity.DcEntity;
import com.ctrip.xpipe.redis.console.mapper.DcMapper;
import jakarta.annotation.Resource;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Repository
public class DcRepository {

    @Resource
    private DcMapper dcMapper;

    public Map<Long, String> getDcIdNameMap() {
        QueryWrapper<DcEntity> wrapper = new QueryWrapper<>();
        wrapper.select(DcEntity.ID, DcEntity.DC_NAME);
        List<DcEntity> dcEntities = dcMapper.selectList(wrapper);
        return dcEntities.stream().collect(Collectors.toMap(DcEntity::getId, DcEntity::getDcName));
    }
}
