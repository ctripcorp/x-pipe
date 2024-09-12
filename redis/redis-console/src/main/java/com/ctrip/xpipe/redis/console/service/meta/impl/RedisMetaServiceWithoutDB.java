package com.ctrip.xpipe.redis.console.service.meta.impl;

import com.ctrip.xpipe.redis.checker.spring.ConsoleDisableDbCondition;
import com.ctrip.xpipe.redis.checker.spring.DisableDbMode;
import com.ctrip.xpipe.redis.console.resources.ConsolePortalService;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Conditional;
import org.springframework.stereotype.Service;

@Service
@Conditional(ConsoleDisableDbCondition.class)
@DisableDbMode(true)
public class RedisMetaServiceWithoutDB extends RedisMetaServiceImpl {

    @Autowired
    private ConsolePortalService consolePortalService;

    @Override
    public void updateKeeperStatus(String dcId, String clusterId, String shardId, KeeperMeta newActiveKeeper) {
        consolePortalService.updateKeeperStatus(dcId, clusterId, shardId, newActiveKeeper);
    }
}
