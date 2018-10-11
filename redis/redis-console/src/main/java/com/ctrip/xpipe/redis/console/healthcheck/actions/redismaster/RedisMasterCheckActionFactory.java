package com.ctrip.xpipe.redis.console.healthcheck.actions.redismaster;

import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.crossdc.AbstractCDLAHealthCheckActionFactory;
import com.ctrip.xpipe.redis.console.healthcheck.crossdc.CrossDcLeaderAwareHealthCheckAction;
import com.ctrip.xpipe.redis.console.service.RedisService;
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

    @Override
    public CrossDcLeaderAwareHealthCheckAction create(RedisHealthCheckInstance instance) {
        return new RedisMasterCheckAction(scheduled, instance, executors, redisService);
    }

    @Override
    public Class<? extends CrossDcLeaderAwareHealthCheckAction> support() {
        return RedisMasterCheckAction.class;
    }

    @Override
    protected List<ALERT_TYPE> alertTypes() {
        return Lists.newArrayList();
    }
}
