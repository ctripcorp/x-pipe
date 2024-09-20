package com.ctrip.xpipe.redis.console.cache.impl;

import com.ctrip.xpipe.redis.checker.spring.ConsoleDisableDbCondition;
import com.ctrip.xpipe.redis.checker.spring.DisableDbMode;
import com.ctrip.xpipe.redis.console.cache.AzGroupCache;
import com.ctrip.xpipe.redis.console.model.AzGroupModel;
import com.ctrip.xpipe.redis.console.resources.ConsolePortalService;
import org.apache.commons.collections.SetUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Conditional;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
@Conditional(ConsoleDisableDbCondition.class)
@DisableDbMode(true)
public class AzGroupCacheWithoutDBImpl implements AzGroupCache {

    private ConsolePortalService consolePortalService;

    @Autowired
    public AzGroupCacheWithoutDBImpl(ConsolePortalService consolePortalService) {
        this.consolePortalService = consolePortalService;
    }

    private List<AzGroupModel> azGroupModels = null;

    private Map<Long, AzGroupModel> idAzGroupMap = null;

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

    private void loadAzGroupCache() {
        this.azGroupModels = consolePortalService.getAllAzGroups();
        this.idAzGroupMap = this.azGroupModels.stream().collect(Collectors.toMap(AzGroupModel::getId, Function.identity()));
    }
}
