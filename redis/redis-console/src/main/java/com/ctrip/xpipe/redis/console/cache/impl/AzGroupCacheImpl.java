package com.ctrip.xpipe.redis.console.cache.impl;

import com.ctrip.xpipe.redis.checker.spring.ConsoleDisableDbCondition;
import com.ctrip.xpipe.redis.checker.spring.DisableDbMode;
import com.ctrip.xpipe.redis.console.cache.AzGroupCache;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.entity.AzGroupEntity;
import com.ctrip.xpipe.redis.console.model.AzGroupModel;
import com.ctrip.xpipe.redis.console.repository.AzGroupMappingRepository;
import com.ctrip.xpipe.redis.console.repository.AzGroupRepository;
import com.ctrip.xpipe.redis.console.repository.DcRepository;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import org.apache.commons.collections.SetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Conditional;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
@Conditional(ConsoleDisableDbCondition.class)
@DisableDbMode(false)
public class AzGroupCacheImpl implements AzGroupCache {

    private ConsoleConfig config;

    private DcRepository dcRepository;

    private AzGroupRepository azGroupRepository;

    private AzGroupMappingRepository azGroupMappingRepository;

    @Autowired
    public AzGroupCacheImpl(ConsoleConfig config,
                            DcRepository dcRepository,
                            AzGroupRepository azGroupRepository,
                            AzGroupMappingRepository azGroupMappingRepository) {
        this.config = config;
        this.dcRepository = dcRepository;
        this.azGroupRepository = azGroupRepository;
        this.azGroupMappingRepository  = azGroupMappingRepository;
    }

    private static final Logger logger = LoggerFactory.getLogger(AzGroupCacheImpl.class.getName());
//    private static final AzGroupModel DEFAULT_AZ_GROUP = new AzGroupModel(0L, "", "", Collections.emptyList());

    private final ScheduledExecutorService executor =
        new ScheduledThreadPoolExecutor(1, XpipeThreadFactory.create(getClass().getSimpleName()));

    private List<AzGroupModel> azGroupModels = null;
    private Map<Long, AzGroupModel> idAzGroupMap = null;

    @VisibleForTesting
    public AzGroupCacheImpl(List<AzGroupModel> azGroupModels) {
        this.azGroupModels = azGroupModels;
    }

    @PostConstruct
    public void init() {
        executor.scheduleWithFixedDelay(this::loadAzGroupCache, 1L, 30L, TimeUnit.MINUTES);
    }

    private void loadAzGroupCache() {
        List<AzGroupEntity> azGroupEntities = azGroupRepository.selectAll();
        Map<Long, List<Long>> azGroupAzsMap = azGroupMappingRepository.getAzGroupAzsMap();
        Map<Long, String> azIdNameMap = dcRepository.getDcIdNameMap();

        List<AzGroupModel> azGroupModels = new ArrayList<>();
        for (AzGroupEntity entity : azGroupEntities) {
            List<Long> azIds = azGroupAzsMap.get(entity.getId());
            List<String> azs = azIds.stream().map(azIdNameMap::get).collect(Collectors.toList());

            AzGroupModel azGroupModel = new AzGroupModel(entity.getId(), entity.getName(), entity.getRegion(), azs);
            azGroupModels.add(azGroupModel);
        }
        this.azGroupModels = azGroupModels;
        this.idAzGroupMap = azGroupModels.stream().collect(Collectors.toMap(AzGroupModel::getId, Function.identity()));
    }

    @Override
    public List<AzGroupModel> getAllAzGroup() {
        if (this.azGroupModels == null) {
            this.loadAzGroupCache();
        }
        return this.azGroupModels;
    }


    @Override
    public AzGroupModel getAzGroupById(Long id) {
        if (this.azGroupModels == null) {
            this.loadAzGroupCache();
        }
        return this.idAzGroupMap.get(id);
//        return idAzGroupMap.getOrDefault(id, DEFAULT_AZ_GROUP);
    }

    @Override
    public AzGroupModel getAzGroupByAzs(List<String> azs) {
        if (this.azGroupModels == null) {
            this.loadAzGroupCache();
        }
        for (AzGroupModel model : this.azGroupModels) {
            if (SetUtils.isEqualSet(model.getAzs(), azs)) {
                return model;
            }
        }
        return null;
    }

    @Override
    public List<AzGroupModel> getAzGroupsByAz(String az) {
        if (az == null) return Collections.emptyList();
        if (this.azGroupModels == null) {
            this.loadAzGroupCache();
        }
        return this.azGroupModels.stream()
                .filter(model -> model.containsAz(az))
                .collect(Collectors.toList());
    }

}
