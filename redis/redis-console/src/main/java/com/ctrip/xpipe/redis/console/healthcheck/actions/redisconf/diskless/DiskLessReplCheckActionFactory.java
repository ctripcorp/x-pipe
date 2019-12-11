package com.ctrip.xpipe.redis.console.healthcheck.actions.redisconf.diskless;

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
public class DiskLessReplCheckActionFactory extends AbstractLeaderAwareHealthCheckActionFactory {

    @Override
    public SiteLeaderAwareHealthCheckAction create(RedisHealthCheckInstance instance) {
        return new DiskLessReplCheckAction(scheduled, instance, executors, alertManager);
    }

    @Override
    public Class<? extends SiteLeaderAwareHealthCheckAction> support() {
        return DiskLessReplCheckAction.class;
    }

    @Override
    protected List<ALERT_TYPE> alertTypes() {
        return Lists.newArrayList(ALERT_TYPE.REDIS_REPL_DISKLESS_SYNC_ERROR);
    }
}
