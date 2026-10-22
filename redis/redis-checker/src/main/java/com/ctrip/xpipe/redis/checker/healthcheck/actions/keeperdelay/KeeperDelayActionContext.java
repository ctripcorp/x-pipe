package com.ctrip.xpipe.redis.checker.healthcheck.actions.keeperdelay;

import com.ctrip.xpipe.redis.checker.healthcheck.AbstractActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.KeeperHealthCheckInstance;
import com.ctrip.xpipe.redis.core.meta.KeeperState;

/**
 * Delay result emitted by a Keeper-only health-check action.
 */
public class KeeperDelayActionContext extends AbstractActionContext<Long, KeeperHealthCheckInstance> {

    /** 采自 INFO replication 的实时状态；未采到时为 null。 */
    private final KeeperState keeperState;

    public KeeperDelayActionContext(KeeperHealthCheckInstance instance, Long delay) {
        this(instance, delay, null);
    }

    public KeeperDelayActionContext(KeeperHealthCheckInstance instance, Long delay, KeeperState keeperState) {
        super(instance, delay);
        this.keeperState = keeperState;
    }

    public KeeperState getKeeperState() {
        return keeperState;
    }
}
