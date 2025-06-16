package com.ctrip.xpipe.redis.console.repository;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.ctrip.xpipe.redis.console.entity.AzGroupEntity;
import com.ctrip.xpipe.redis.console.mapper.AzGroupMapper;
import org.springframework.stereotype.Repository;

import javax.annotation.Resource;
import java.util.List;

@Repository
public class AzGroupRepository {

    @Resource
    private AzGroupMapper azGroupMapper;

    public List<AzGroupEntity> selectAll() {
        return azGroupMapper.selectList(null);
    }

    public AzGroupEntity selectById(Long id) {
        if (id == null) {
            return null;
        }
        return azGroupMapper.selectById(id);
    }

    public Long selectIdByName(String name) {
        QueryWrapper<AzGroupEntity> wrapper = new QueryWrapper<>();
        wrapper.select(AzGroupEntity.ID).eq(AzGroupEntity.NAME, name);
        AzGroupEntity azGroup = azGroupMapper.selectOne(wrapper);
        return azGroup == null ? null : azGroup.getId();
    }

    public String selectNameById(Long id) {
        QueryWrapper<AzGroupEntity> wrapper = new QueryWrapper<>();
        wrapper.select(AzGroupEntity.NAME).eq(AzGroupEntity.ID, id);
        return azGroupMapper.selectOne(wrapper).getName();
    }

}
