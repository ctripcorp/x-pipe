package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisconf.diskless;

import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.healthcheck.OneWaySupport;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.leader.AbstractRedisLeaderAwareHealthCheckActionFactory;
import com.ctrip.xpipe.redis.checker.healthcheck.leader.SiteLeaderAwareHealthCheckAction;
import com.google.common.collect.Lists;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Oct 08, 2018
 */

@Component
public class DiskLessReplCheckActionFactory extends AbstractRedisLeaderAwareHealthCheckActionFactory implements OneWaySupport {

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
