package com.ctrip.xpipe.redis.core.route.impl;

import com.ctrip.xpipe.redis.core.route.RouteChooseStrategy;
import com.ctrip.xpipe.redis.core.route.RouteChooseStrategyFactory;
import org.springframework.stereotype.Component;

@Component
public class DefaultRouteChooseStrategyFactory implements RouteChooseStrategyFactory {

    public DefaultRouteChooseStrategyFactory() {
    }

    public RouteChooseStrategy create(RouteStrategyType routeStrategyType, String clusterName) {
        switch (routeStrategyType) {
            case Crc32Hash:
            default:
                return new Crc32HashRouteChooseStrategy(clusterName);
        }
    }
}
