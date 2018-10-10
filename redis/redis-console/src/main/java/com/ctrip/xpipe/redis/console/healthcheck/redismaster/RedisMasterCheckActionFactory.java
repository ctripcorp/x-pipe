package com.ctrip.xpipe.redis.console.healthcheck.redismaster;

import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.redisconf.AbstractCDLAHealthCheckActionFactory;
import com.ctrip.xpipe.redis.console.healthcheck.redisconf.CrossDcLeaderAwareHealthCheckAction;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.redis.console.service.RedisService;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Oct 09, 2018
 */
@Component
public class RedisMasterCheckActionFactory extends AbstractCDLAHealthCheckActionFactory {

    @Autowired
    private RedisService redisService;

    @Autowired
    private MetaCache metaCache;

    @Override
    public CrossDcLeaderAwareHealthCheckAction create(RedisHealthCheckInstance instance) {
        return new RedisMasterCheckAction(scheduled, instance, executors, metaCache, redisService);
    }

    @Override
    public Class<? extends CrossDcLeaderAwareHealthCheckAction> support() {
        return RedisMasterCheckAction.class;
    }

    @VisibleForTesting
    protected RedisMasterCheckActionFactory setMetaCache(MetaCache metaCache) {
        this.metaCache = metaCache;
        return this;
    }

    @Override
    protected List<ALERT_TYPE> alertTypes() {
        return Lists.newArrayList();
    }
}
