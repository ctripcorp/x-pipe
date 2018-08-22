package com.ctrip.xpipe.redis.console.health;

import com.ctrip.xpipe.api.proxy.ProxyEnabled;
import com.ctrip.xpipe.api.proxy.ProxyProtocol;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.protocal.cmd.AbstractRedisCommand;

/**
 * @author chen.zhu
 * <p>
 * Aug 21, 2018
 */
public class DefaultProxyEnabledHealthCheckEndpoint extends AbstractHealthCheckEndpoint implements ProxyEnabled {

    private ProxyProtocol protocol;

    public DefaultProxyEnabledHealthCheckEndpoint(RedisMeta redisMeta, ProxyProtocol protocol) {
        super(redisMeta);
        this.protocol = protocol;
    }

    @Override
    public int getDelayCheckTimeoutMilli() {
        return 30 * 1000;
    }

    @Override
    public int getCommandTimeoutMilli() {
        return AbstractRedisCommand.PROXYED_REDIS_CONNECTION_COMMAND_TIME_OUT_MILLI;
    }

    @Override
    public ProxyProtocol getProxyProtocol() {
        return protocol;
    }

    @Override
    public String toString() {
        return getHostPort() + "protocol: " + protocol.getContent();
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }
}
