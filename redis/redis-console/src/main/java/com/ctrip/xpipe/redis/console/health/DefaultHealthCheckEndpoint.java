package com.ctrip.xpipe.redis.console.health;

import com.ctrip.xpipe.redis.core.entity.RedisMeta;

/**
 * @author chen.zhu
 * <p>
 * Aug 21, 2018
 */
public class DefaultHealthCheckEndpoint extends AbstractHealthCheckEndpoint {

    public DefaultHealthCheckEndpoint(RedisMeta redisMeta) {
        super(redisMeta);
    }

    @Override
    public int getDelayCheckTimeoutMilli() {
        return 1500;
    }

    @Override
    public String toString() {
        return getHostPort().toString();
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }
}
