package com.ctrip.xpipe.redis.core.route;

import com.ctrip.xpipe.redis.core.route.impl.Crc32HashRouteChooseStrategy;

public class RouteChooseStrategyFactory {
    public static RouteChooseStrategyFactory DEFAULT = new RouteChooseStrategyFactory();

    private static final String CRC32_HASH_ROUTE_CHOOSE_STRATEGY =  "crc32Hash";

    public RouteChooseStrategy createRouteStrategy(String strategyName, String clusterName) {
        if (strategyName.equalsIgnoreCase(CRC32_HASH_ROUTE_CHOOSE_STRATEGY))
            return new Crc32HashRouteChooseStrategy(clusterName);
        else
            return new Crc32HashRouteChooseStrategy(clusterName);
    }
}
