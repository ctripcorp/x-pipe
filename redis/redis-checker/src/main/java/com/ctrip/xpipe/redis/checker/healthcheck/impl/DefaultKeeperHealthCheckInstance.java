package com.ctrip.xpipe.redis.checker.healthcheck.impl;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.redis.checker.healthcheck.KeeperHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.KeeperInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.session.RedisSession;
import com.ctrip.xpipe.utils.ObjectUtils;

public class DefaultKeeperHealthCheckInstance extends AbstractHealthCheckInstance<KeeperInstanceInfo> implements KeeperHealthCheckInstance {

    private Endpoint endpoint;

    private RedisSession session;

    @Override
    public Endpoint getEndpoint() {
        return endpoint;
    }



    public DefaultKeeperHealthCheckInstance setEndpoint(Endpoint endpoint) {
        this.endpoint = endpoint;
        return this;
    }

    @Override
    public RedisSession getRedisSession() {
        return session;
    }

    public DefaultKeeperHealthCheckInstance setSession(RedisSession session) {
        this.session = session;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DefaultKeeperHealthCheckInstance that = (DefaultKeeperHealthCheckInstance) o;
        return ObjectUtils.equals(that.getCheckInfo().getHostPort(),
                this.getCheckInfo().getHostPort());
    }

    @Override
    public int hashCode() {
        return getCheckInfo().getHostPort().hashCode();
    }

    @Override
    public String toString() {
        return String.format("HealthCheckInstanceInfo: [%s]", getCheckInfo().toString());
    }

}
