package com.ctrip.xpipe.redis.console.healthcheck.redisconf.version;

import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.redisconf.AbstractCDLAHealthCheckActionFactory;
import com.ctrip.xpipe.redis.console.healthcheck.redisconf.CrossDcLeaderAwareHealthCheckAction;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Oct 08, 2018
 */
@Component
public class VersionCheckActionFactory extends AbstractCDLAHealthCheckActionFactory {

    @Autowired
    private MetaCache metaCache;

    @Override
    public CrossDcLeaderAwareHealthCheckAction create(RedisHealthCheckInstance instance) {
        return new VersionCheckAction(scheduled, instance, executors, alertManager, metaCache);
    }

    @Override
    public Class<? extends CrossDcLeaderAwareHealthCheckAction> support() {
        return VersionCheckAction.class;
    }

    @Override
    protected List<ALERT_TYPE> alertTypes() {
        return Lists.newArrayList(ALERT_TYPE.XREDIS_VERSION_NOT_VALID);
    }
}
