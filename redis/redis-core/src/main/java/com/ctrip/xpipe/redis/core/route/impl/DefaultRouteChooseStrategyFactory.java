package com.ctrip.xpipe.redis.core.route.impl;

import com.ctrip.xpipe.redis.core.route.RouteChooseStrategy;
import com.ctrip.xpipe.redis.core.route.RouteChooseStrategyFactory;

public class DefaultRouteChooseStrategyFactory implements RouteChooseStrategyFactory {
    public static final String CRC32_HASH_ROUTE_CHOOSE_STRATEGY =  "crc32Hash";

    public RouteChooseStrategy createRouteStrategy(String strategyName, String clusterName) {
        if(strategyName.equalsIgnoreCase(CRC32_HASH_ROUTE_CHOOSE_STRATEGY))
            return new Crc32HashRouteChooseStrategy(clusterName);
        else
            return new Crc32HashRouteChooseStrategy(clusterName);
    }
}
