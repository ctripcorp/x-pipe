package com.ctrip.xpipe.redis.checker.healthcheck.session;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.healthcheck.meta.KeeperCheckSelector;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.Set;

/**
 * Owns Checker sessions for Keepers independently from Redis sessions.
 *
 * The in-use set mirrors the instance loading rules of {@link KeeperCheckSelector}, so it holds
 * exactly the Keepers that get a health-check instance -- including same-region non-TFS Keepers,
 * and regardless of {@code keeper.delay.check.enabled}, which never takes part in instance,
 * session or capability-cache lifecycle.
 */
@Component
public class DefaultKeeperSessionManager extends AbstractInstanceSessionManager implements KeeperSessionManager {

    @Autowired
    private KeeperCheckSelector keeperSelector;

    @Override
    public RedisSession findOrCreateSession(HostPort hostPort) {
        return findOrCreateSession(new DefaultEndPoint(hostPort.getHost(), hostPort.getPort()));
    }

    /**
     * Unlike the Redis manager, an empty in-use set here is a precise statement: the selector
     * matched no Keeper at all, so every Keeper session must be recycled rather than kept alive
     * until the meta happens to grow again.
     */
    @Override
    protected boolean cleanUpOnEmptyInUseInstances() {
        return true;
    }

    @Override
    protected Set<HostPort> getInUseInstances() {
        XpipeMeta xpipeMeta = metaCache.getXpipeMeta();
        // null / empty meta means the expectation is unknown, not "nothing is in use"
        if (xpipeMeta == null || xpipeMeta.getDcs() == null || xpipeMeta.getDcs().isEmpty()) {
            return null;
        }

        Set<HostPort> keepersInUse = new HashSet<>();
        for (KeeperMeta keeperMeta : keeperSelector.select(xpipeMeta)) {
            keepersInUse.add(new HostPort(keeperMeta.getIp(), keeperMeta.getPort()));
        }
        return keepersInUse;
    }

    @VisibleForTesting
    public DefaultKeeperSessionManager setKeeperSelector(KeeperCheckSelector keeperSelector) {
        this.keeperSelector = keeperSelector;
        return this;
    }
}
