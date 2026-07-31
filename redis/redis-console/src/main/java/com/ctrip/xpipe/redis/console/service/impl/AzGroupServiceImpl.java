package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.cache.AzGroupCache;
import com.ctrip.xpipe.redis.console.entity.AzGroupClusterEntity;
import com.ctrip.xpipe.redis.console.entity.AzGroupEntity;
import com.ctrip.xpipe.redis.console.exception.BadRequestException;
import com.ctrip.xpipe.redis.console.model.AzGroupModel;
import com.ctrip.xpipe.redis.console.repository.AzGroupClusterRepository;
import com.ctrip.xpipe.redis.console.repository.AzGroupMappingRepository;
import com.ctrip.xpipe.redis.console.repository.AzGroupRepository;
import com.ctrip.xpipe.redis.console.service.AzGroupService;
import com.ctrip.xpipe.redis.console.service.DcService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Service
public class AzGroupServiceImpl implements AzGroupService {

    private static final Logger logger = LoggerFactory.getLogger(AzGroupServiceImpl.class);

    @Resource
    private AzGroupRepository azGroupRepository;

    @Resource
    private AzGroupMappingRepository azGroupMappingRepository;

    @Resource
    private AzGroupClusterRepository azGroupClusterRepository;

    @Resource
    private AzGroupCache azGroupCache;

    @Resource
    private DcService dcService;

    @Override
    @Transactional
    public void create(String name, List<String> azs) {
        if (StringUtils.isEmpty(name) || CollectionUtils.isEmpty(azs)) {
            throw new BadRequestException("name and azs are required");
        }

        if (azGroupRepository.selectIdByName(name) != null) {
            throw new BadRequestException(String.format("az group name %s already exists", name));
        }

        Map<String, Long> dcNameIdMap = dcService.dcNameIdMap();
        List<Long> azIds = new ArrayList<>(azs.size());
        for (String az : azs) {
            Long azId = dcNameIdMap.get(az);
            if (azId == null) {
                throw new BadRequestException(String.format("az - %s does not exist", az));
            }
            azIds.add(azId);
        }

        AzGroupModel existing = azGroupCache.getAzGroupByAzs(azs);
        if (existing != null) {
            throw new BadRequestException(String.format(
                    "azs %s already used by az group %s", azs, existing.getName()));
        }

        Long azGroupId = azGroupRepository.insert(name);
        azGroupMappingRepository.batchInsert(azGroupId, azIds);
        azGroupCache.reload();

        logger.info("[create][azGroup] name={}, azGroupId={}, azs={}", name, azGroupId, azs);
    }

    @Override
    @Transactional
    public void deleteByName(String name) {
        if (StringUtils.isEmpty(name)) {
            throw new BadRequestException("name is required");
        }

        AzGroupEntity azGroup = azGroupRepository.selectByName(name);
        if (azGroup == null) {
            throw new BadRequestException(String.format("az group %s not found", name));
        }

        Long azGroupId = azGroup.getId();
        List<AzGroupClusterEntity> refs =
                azGroupClusterRepository.selectByAzGroupIds(Collections.singletonList(azGroupId));
        if (!CollectionUtils.isEmpty(refs)) {
            throw new BadRequestException(String.format(
                    "az group %s is still referenced by az_group_cluster, can not be deleted", name));
        }

        AzGroupModel cached = azGroupCache.getAzGroupById(azGroupId);
        List<String> azs = cached == null ? Collections.emptyList() : cached.getAzsAsList();

        azGroupMappingRepository.deleteByAzGroupId(azGroupId);
        azGroupRepository.deleteById(azGroupId);
        azGroupCache.reload();

        logger.info("[deleteByName][azGroup] name={}, azGroupId={}, azs={}", name, azGroupId, azs);
    }

    @Override
    public List<AzGroupModel> getAll() {
        return azGroupCache.getAllAzGroup();
    }

}
