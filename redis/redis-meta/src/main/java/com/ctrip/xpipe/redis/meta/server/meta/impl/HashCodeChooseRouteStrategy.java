package com.ctrip.xpipe.redis.meta.server.meta.impl;

import com.ctrip.xpipe.redis.core.entity.RouteMeta;
import com.ctrip.xpipe.redis.meta.server.meta.ChooseRouteStrategy;

import java.util.List;

public class HashCodeChooseRouteStrategy implements ChooseRouteStrategy {
    private int hashCode;
    public HashCodeChooseRouteStrategy(int code) {
        this.hashCode = code;
    }
    @Override
    public RouteMeta choose(List<RouteMeta> routeMetas) {
        if(routeMetas == null || routeMetas.isEmpty()) {
            return null;
        }
        return routeMetas.get(Math.abs(hashCode) % routeMetas.size());
    }

    public void setCode(int hashCode) {
        this.hashCode = hashCode;
    }

    public int getCode() {
        return hashCode;
    }
}
