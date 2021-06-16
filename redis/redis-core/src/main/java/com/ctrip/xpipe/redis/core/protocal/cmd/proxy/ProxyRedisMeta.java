package com.ctrip.xpipe.redis.core.protocal.cmd.proxy;

import com.ctrip.xpipe.redis.core.entity.RedisMeta;

import java.util.Objects;

public class ProxyRedisMeta extends RedisMeta {
    RedisProxy proxy;

    public ProxyRedisMeta setProxy(RedisProxy proxy) {
        this.proxy = proxy;
        return this;
    }

    public RedisProxy getProxy() {
        return proxy;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        ProxyRedisMeta that = (ProxyRedisMeta) o;
        return Objects.equals(proxy, that.proxy);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), proxy);
    }

    public static ProxyRedisMeta valueof(RedisMeta meta) {
        return (ProxyRedisMeta) new ProxyRedisMeta().setGid(meta.getGid()).setId(meta.getId()).setIp(meta.getIp()).setPort(meta.getPort());
    }
}
