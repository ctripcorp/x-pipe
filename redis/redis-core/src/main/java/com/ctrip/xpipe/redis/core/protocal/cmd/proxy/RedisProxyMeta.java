package com.ctrip.xpipe.redis.core.protocal.cmd.proxy;

import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.RouteMeta;
import com.ctrip.xpipe.redis.core.protocal.cmd.proxy.impl.XpipeRedisProxy;

import java.util.Objects;

public class RedisProxyMeta extends RedisMeta {
    RedisProxy proxy;

    public RedisProxyMeta setProxy(RedisProxy proxy) {
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
        RedisProxyMeta that = (RedisProxyMeta) o;
        return Objects.equals(proxy, that.proxy);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), proxy);
    }

    public static RedisProxyMeta create(RedisMeta redisMeta, RouteMeta routeMeta) {
        RedisProxyMeta proxyMeta = new RedisProxyMeta();
        proxyMeta.setGid(redisMeta.getGid()).setId(redisMeta.getId()).setIp(redisMeta.getIp()).setPort(redisMeta.getPort());
        if(routeMeta != null) {
            proxyMeta.setProxy(RedisProxyFactory.create(routeMeta));
        }
        return proxyMeta;
    }
}
