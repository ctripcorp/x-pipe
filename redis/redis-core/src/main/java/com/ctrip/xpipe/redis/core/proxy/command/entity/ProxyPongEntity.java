package com.ctrip.xpipe.redis.core.proxy.command.entity;

import com.ctrip.xpipe.endpoint.HostPort;

import java.util.Objects;

/**
 * @author chen.zhu
 * <p>
 * Oct 24, 2018
 */
public class ProxyPongEntity {

    private HostPort direct;

    private HostPort real;

    private long rtt;

    public ProxyPongEntity(HostPort direct) {
        this.direct = direct;
    }

    public ProxyPongEntity(HostPort direct, HostPort real, long rtt) {
        this.direct = direct;
        this.real = real;
        this.rtt = rtt;
    }

    public HostPort getDirect() {
        return direct;
    }

    public HostPort getReal() {
        return real;
    }

    public long getRtt() {
        return rtt;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProxyPongEntity that = (ProxyPongEntity) o;
        return rtt == that.rtt &&
                Objects.equals(direct, that.direct) &&
                Objects.equals(real, that.real);
    }

    @Override
    public int hashCode() {

        return Objects.hash(direct, real, rtt);
    }
}
