package com.ctrip.xpipe.redis.console.service.meta.impl;

import org.apache.commons.lang3.SerializationUtils;
import com.ctrip.xpipe.redis.checker.spring.ConsoleDisableDbCondition;
import com.ctrip.xpipe.redis.checker.spring.DisableDbMode;
import com.ctrip.xpipe.redis.console.service.meta.DcMetaService;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Conditional;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Service
@Conditional(ConsoleDisableDbCondition.class)
@DisableDbMode(true)
public class DcMetaServiceWithoutDB implements DcMetaService {

    @Autowired
    private MetaCache metaCache;

    @Override
    public DcMeta getDcMeta(String dcName) throws Exception {
        return metaCache.getXpipeMeta().getDcs().get(dcName);
    }

    @Override
    public DcMeta getDcMeta(String dcName, Set<String> allowTypes) throws Exception {
        // 大小写

        Set<String> upperCaseAllowTypes = allowTypes.stream()
                .map(String::toUpperCase)
                .collect(Collectors.toSet());

        DcMeta dcMeta = metaCache.getXpipeMeta().getDcs().get(dcName);
        DcMeta result = SerializationUtils.clone(dcMeta);
        result.getClusters().clear();
        Map<String, ClusterMeta> clusterMetas = dcMeta.getClusters();
        for(ClusterMeta clusterMeta : clusterMetas.values()) {
            if(upperCaseAllowTypes.contains(clusterMeta.getType().toUpperCase())) {
                result.addCluster(SerializationUtils.clone(clusterMeta));
            }
        }
        return result;
    }

    @Override
    public Map<String, DcMeta> getAllDcMetas() throws Exception {
        return metaCache.getXpipeMeta().getDcs();
    }
}
