package com.ctrip.xpipe.redis.meta.server.meta.impl;

import com.ctrip.xpipe.redis.core.entity.RouteMeta;
import com.ctrip.xpipe.redis.meta.server.meta.ChooseRouteStrategy;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.nio.charset.Charset;
import java.util.List;

public class Crc32HashChooseRouteStrategy implements ChooseRouteStrategy {
    private int hashCode;

    private static HashFunction crc32HashFunction = Hashing.crc32();

    public Crc32HashChooseRouteStrategy(String clusterName) {
        this.hashCode = crc32HashFunction.hashString(clusterName, Charset.forName("utf-8")).asInt();
    }

    public void setCode(int hashCode) {
        this.hashCode = hashCode;
    }

    public int getCode() {
        return hashCode;
    }

    @Override
    public RouteMeta choose(List<RouteMeta> routeMetas) {
        if(routeMetas == null || routeMetas.isEmpty()) {
            return null;
        }
        return routeMetas.get(Math.abs(hashCode) % routeMetas.size());
    }
}
