package com.ctrip.xpipe.redis.core.route.impl;

import com.ctrip.xpipe.redis.core.entity.RouteMeta;
import com.ctrip.xpipe.redis.core.route.RouteChooseStrategy;
import com.ctrip.xpipe.redis.core.route.RouteChooseStrategyFactory;
import com.ctrip.xpipe.utils.StringUtil;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.nio.charset.Charset;
import java.util.List;

public class Crc32HashRouteChooseStrategy implements RouteChooseStrategy {

    private static HashFunction crc32HashFunction = Hashing.crc32();

    private RouteChooseStrategyFactory.RouteStrategyType routeStrategyType;

    public Crc32HashRouteChooseStrategy(RouteChooseStrategyFactory.RouteStrategyType routeStrategyType) {
        this.routeStrategyType = routeStrategyType;
    }

    @Override
    public RouteChooseStrategyFactory.RouteStrategyType getRouteStrategyType() {
        return routeStrategyType;
    }

    @Override
    public RouteMeta choose(List<RouteMeta> routeMetas, String clusterName) {
        if (routeMetas == null || routeMetas.isEmpty()) {
            return null;
        }

        if(StringUtil.isEmpty(clusterName)) {
            throw new IllegalArgumentException("empty cluster name");
        }

        int hashCode = crc32HashFunction.hashString(clusterName, Charset.forName("utf-8")).asInt();
        return routeMetas.get(Math.abs(hashCode) % routeMetas.size());
    }
}
