package com.ctrip.xpipe.redis.core.protocal.cmd.proxy;

import com.ctrip.xpipe.redis.core.entity.RouteMeta;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import com.ctrip.xpipe.redis.core.protocal.cmd.proxy.impl.XpipeRedisProxy;

public class RedisProxyFactory {
    public static RedisProxy create(RouteMeta routeMeta) {
        if(routeMeta == null) return null;
        String routeInfo = routeMeta.getRouteInfo();
        XpipeRedisProxy proxy = XpipeRedisProxy.read(routeInfo);
        return proxy;
    }

    static String TEMP_PEER_TYPE = "peer%d_proxy_type";
    public static RedisProxy valueofInfo(InfoResultExtractor e, int index) {
        String type = e.extract(String.format(TEMP_PEER_TYPE, index));
        for (RedisProxyType temp : RedisProxyType.values()) {
            RedisProxy p = temp.parse(type, e, index);
            if(p != null) {
                return p;
            }
        }
        return null;
    }
}
