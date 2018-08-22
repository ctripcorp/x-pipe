package com.ctrip.xpipe.redis.console.health;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.protocal.cmd.AbstractRedisCommand;

import java.util.Objects;

/**
 * @author chen.zhu
 * <p>
 * Aug 21, 2018
 */
public abstract class AbstractHealthCheckEndpoint extends DefaultEndPoint implements HealthCheckEndpoint {

    protected RedisMeta redisMeta;

    protected HostPort hostPort;

    public AbstractHealthCheckEndpoint(RedisMeta redisMeta) {
        super(redisMeta.getIp(), redisMeta.getPort());
        this.redisMeta = redisMeta;
        this.hostPort = new HostPort(redisMeta.getIp(), redisMeta.getPort());
    }

    @Override
    public int getCommandTimeoutMilli() {
        return AbstractRedisCommand.DEFAULT_REDIS_COMMAND_TIME_OUT_MILLI;
    }

    @Override
    public RedisMeta getRedisMeta() {
        return redisMeta;
    }

    @Override
    public HostPort getHostPort() {
        return hostPort;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        AbstractHealthCheckEndpoint that = (AbstractHealthCheckEndpoint) o;
        return Objects.equals(hostPort, that.hostPort);
    }

    @Override
    public int hashCode() {

        return Objects.hash(super.hashCode(), hostPort);
    }
}
