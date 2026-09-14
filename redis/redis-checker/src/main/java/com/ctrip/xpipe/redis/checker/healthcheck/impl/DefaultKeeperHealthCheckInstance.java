package com.ctrip.xpipe.redis.checker.healthcheck.impl;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.redis.checker.healthcheck.KeeperHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.KeeperInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.session.RedisSession;
import com.ctrip.xpipe.utils.ObjectUtils;

/**
 * Default Keeper health-check instance. Keeper actions are installed separately
 * from Redis actions.
 */
public class DefaultKeeperHealthCheckInstance extends AbstractHealthCheckInstance<KeeperInstanceInfo>
        implements KeeperHealthCheckInstance {

    private Endpoint endpoint;

    private RedisSession session;

    public DefaultKeeperHealthCheckInstance setEndpoint(Endpoint endpoint) {
        this.endpoint = endpoint;
        return this;
    }

    public DefaultKeeperHealthCheckInstance setSession(RedisSession session) {
        this.session = session;
        return this;
    }

    @Override
    public Endpoint getEndpoint() {
        return endpoint;
    }

    @Override
    public RedisSession getRedisSession() {
        return session;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        DefaultKeeperHealthCheckInstance that = (DefaultKeeperHealthCheckInstance) obj;
        return ObjectUtils.equals(getCheckInfo().getHostPort(), that.getCheckInfo().getHostPort());
    }

    @Override
    public int hashCode() {
        return getCheckInfo().getHostPort().hashCode();
    }

    @Override
    public String toString() {
        return "KeeperHealthCheckInstance[" + getCheckInfo() + "]";
    }
}
