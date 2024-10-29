package com.ctrip.xpipe.redis.console.service.meta.impl;

import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.resources.ConsolePortalService;
import com.ctrip.xpipe.redis.checker.spring.ConsoleDisableDbCondition;
import com.ctrip.xpipe.redis.checker.spring.DisableDbMode;
import com.ctrip.xpipe.redis.console.service.meta.DcMetaService;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Conditional;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Set;

@Service
@Conditional(ConsoleDisableDbCondition.class)
@DisableDbMode(true)
public class DcMetaServiceWithoutDB implements DcMetaService {

    @Autowired
    private MetaCache metaCache;

    @Autowired
    private ConsoleConfig consoleConfig;

    @Autowired
    private ConsolePortalService consolePortalService;

    @Override
    public DcMeta getDcMeta(String dcName) throws Exception {
        return getDcMeta(dcName, consoleConfig.getOwnClusterType());
    }

    @Override
    public DcMeta getDcMeta(String dcName, Set<String> allowTypes) throws Exception {
        return consolePortalService.getDcMeta(dcName, allowTypes);
    }

    @Override
    public Map<String, DcMeta> getAllDcMetas() throws Exception {
        return metaCache.getXpipeMeta().getDcs();
    }
}
