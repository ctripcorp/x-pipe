package com.ctrip.xpipe.redis.console.repository;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.ctrip.xpipe.redis.console.entity.AzGroupEntity;
import com.ctrip.xpipe.redis.console.mapper.AzGroupMapper;
import org.springframework.stereotype.Repository;
import org.springframework.util.StringUtils;

import javax.annotation.Resource;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

@Repository
public class AzGroupRepository {

    /**
     * Matches {@code az_group_tbl.name} varchar(20). Soft-delete renames for uk_name_deleted.
     */
    private static final int MAX_NAME_SIZE = 20;

    private static final String DELETED_NAME_SPLIT_TAG = "-";

    private static final String DELETED_NAME_DATE_PATTERN = "yyyyMMdd";

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

    public AzGroupEntity selectByName(String name) {
        if (StringUtils.isEmpty(name)) {
            return null;
        }
        QueryWrapper<AzGroupEntity> wrapper = new QueryWrapper<>();
        wrapper.eq(AzGroupEntity.NAME, name);
        return azGroupMapper.selectOne(wrapper);
    }

    public Long selectIdByName(String name) {
        AzGroupEntity azGroup = selectByName(name);
        return azGroup == null ? null : azGroup.getId();
    }

    public String selectNameById(Long id) {
        QueryWrapper<AzGroupEntity> wrapper = new QueryWrapper<>();
        wrapper.select(AzGroupEntity.NAME).eq(AzGroupEntity.ID, id);
        return azGroupMapper.selectOne(wrapper).getName();
    }

    public Long insert(String name) {
        AzGroupEntity entity = new AzGroupEntity().setName(name);
        azGroupMapper.insert(entity);
        return entity.getId();
    }

    /**
     * Soft-delete: rename with date suffix first to free {@code uk_name_deleted (name, deleted)},
     * then mark deleted (same pattern as {@code ClusterDao#deleteCluster} / {@code AzDao#deleteAvailableZone}).
     */
    public void deleteById(Long id) {
        if (id == null) {
            return;
        }
        AzGroupEntity entity = azGroupMapper.selectById(id);
        if (entity == null) {
            return;
        }
        entity.setName(generateDeletedName(entity.getName()));
        azGroupMapper.updateById(entity);
        azGroupMapper.deleteById(id);
    }

    /**
     * {@code {origin}-{yyyyMMdd}}, truncate origin (keep suffix) so name fits varchar(20).
     */
    private String generateDeletedName(String originName) {
        String suffix = DELETED_NAME_SPLIT_TAG + new SimpleDateFormat(DELETED_NAME_DATE_PATTERN).format(new Date());
        int maxOriginLen = MAX_NAME_SIZE - suffix.length();
        String origin = originName == null ? "" : originName;
        if (origin.length() > maxOriginLen) {
            origin = origin.substring(0, maxOriginLen);
        }
        return origin + suffix;
    }

}
