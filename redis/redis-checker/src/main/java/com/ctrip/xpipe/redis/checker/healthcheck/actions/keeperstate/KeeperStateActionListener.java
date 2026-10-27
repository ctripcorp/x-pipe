package com.ctrip.xpipe.redis.checker.healthcheck.actions.keeperstate;

import com.ctrip.xpipe.redis.checker.healthcheck.ActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckActionListener;
import com.ctrip.xpipe.redis.checker.healthcheck.KeeperHealthCheckInstance;

/**
 * Listener boundary for Keeper state results.
 */
public interface KeeperStateActionListener extends HealthCheckActionListener<KeeperStateActionContext,
        HealthCheckAction<KeeperHealthCheckInstance>> {

    @Override
    default boolean worksfor(ActionContext context) {
        return context instanceof KeeperStateActionContext;
    }

    boolean supportInstance(KeeperHealthCheckInstance instance);
}
