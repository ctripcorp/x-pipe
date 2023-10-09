package com.ctrip.xpipe.redis.console.cache.impl;

import com.ctrip.xpipe.redis.console.cache.AzGroupCache;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.entity.AzGroupEntity;
import com.ctrip.xpipe.redis.console.model.AzGroupModel;
import com.ctrip.xpipe.redis.console.repository.AzGroupMappingRepository;
import com.ctrip.xpipe.redis.console.repository.AzGroupRepository;
import com.ctrip.xpipe.redis.console.repository.DcRepository;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import org.apache.commons.collections.SetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
public class AzGroupCacheImpl implements AzGroupCache {

    @Autowired
    private ConsoleConfig config;
    @Autowired
    private DcRepository dcRepository;
    @Autowired
    private AzGroupRepository azGroupRepository;
    @Autowired
    private AzGroupMappingRepository azGroupMappingRepository;

    private static final Logger logger = LoggerFactory.getLogger(AzGroupCacheImpl.class.getName());
//    private static final AzGroupModel DEFAULT_AZ_GROUP = new AzGroupModel(0L, "", "", Collections.emptyList());

    private final ScheduledExecutorService executor =
        new ScheduledThreadPoolExecutor(1, XpipeThreadFactory.create(getClass().getSimpleName()));

    private List<AzGroupModel> azGroupModels = null;
    private Map<Long, AzGroupModel> idAzGroupMap = null;

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

}
