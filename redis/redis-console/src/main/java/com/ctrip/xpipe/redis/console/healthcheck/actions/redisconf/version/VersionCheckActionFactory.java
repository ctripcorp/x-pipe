package com.ctrip.xpipe.redis.console.healthcheck.actions.redisconf.version;

import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.leader.AbstractLeaderAwareHealthCheckActionFactory;
import com.ctrip.xpipe.redis.console.healthcheck.leader.SiteLeaderAwareHealthCheckAction;
import com.google.common.collect.Lists;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Oct 08, 2018
 */
@Component
public class VersionCheckActionFactory extends AbstractLeaderAwareHealthCheckActionFactory {

    @Override
    public SiteLeaderAwareHealthCheckAction create(RedisHealthCheckInstance instance) {
        return new VersionCheckAction(scheduled, instance, executors, alertManager);
    }

    @Override
    public Class<? extends SiteLeaderAwareHealthCheckAction> support() {
        return VersionCheckAction.class;
    }

    @Override
    protected List<ALERT_TYPE> alertTypes() {
        return Lists.newArrayList(ALERT_TYPE.XREDIS_VERSION_NOT_VALID);
    }
}
