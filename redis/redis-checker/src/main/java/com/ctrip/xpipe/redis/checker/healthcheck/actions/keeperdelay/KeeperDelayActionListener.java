package com.ctrip.xpipe.redis.checker.healthcheck.actions.keeperdelay;

import com.ctrip.xpipe.redis.checker.healthcheck.ActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckActionListener;
import com.ctrip.xpipe.redis.checker.healthcheck.KeeperHealthCheckInstance;

/**
 * Listener boundary for Keeper delay results. Redis delay contexts cannot pass
 * this compile-time type boundary.
 */
public interface KeeperDelayActionListener extends HealthCheckActionListener<KeeperDelayActionContext,
        HealthCheckAction<KeeperHealthCheckInstance>> {

    @Override
    default boolean worksfor(ActionContext context) {
        return context instanceof KeeperDelayActionContext;
    }

    boolean supportInstance(KeeperHealthCheckInstance instance);
}
