package com.ctrip.xpipe.redis.checker.healthcheck.actions.keeperdelay;

import com.ctrip.xpipe.redis.checker.healthcheck.AbstractActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.KeeperHealthCheckInstance;

/**
 * Delay result emitted by a Keeper-only health-check action.
 */
public class KeeperDelayActionContext extends AbstractActionContext<Long, KeeperHealthCheckInstance> {

    public KeeperDelayActionContext(KeeperHealthCheckInstance instance, Long delay) {
        super(instance, delay);
    }
}
