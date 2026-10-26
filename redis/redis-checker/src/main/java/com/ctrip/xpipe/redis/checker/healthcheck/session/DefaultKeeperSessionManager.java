package com.ctrip.xpipe.redis.checker.healthcheck.session;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.healthcheck.keeperstate.KeeperStateCache;
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
 *
 * Recycling a session also drops the Keeper's entry in {@link KeeperStateCache}: entries are only
 * written through this Keeper's session, so the session's lifetime bounds the entry's.
 */
@Component
public class DefaultKeeperSessionManager extends AbstractInstanceSessionManager implements KeeperSessionManager {

    @Autowired
    private KeeperCheckSelector keeperSelector;

    @Autowired
    private KeeperStateCache keeperStateCache;

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

    @Override
    protected void onSessionRecycled(Endpoint endpoint) {
        if (keeperStateCache != null) {
            keeperStateCache.remove(new HostPort(endpoint.getHost(), endpoint.getPort()));
        }
    }

    @VisibleForTesting
    public DefaultKeeperSessionManager setKeeperSelector(KeeperCheckSelector keeperSelector) {
        this.keeperSelector = keeperSelector;
        return this;
    }

    @VisibleForTesting
    public DefaultKeeperSessionManager setKeeperStateCache(KeeperStateCache keeperStateCache) {
        this.keeperStateCache = keeperStateCache;
        return this;
    }
}
