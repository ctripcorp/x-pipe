package com.ctrip.xpipe.redis.core.route.impl;

import com.ctrip.xpipe.redis.core.route.RouteChooseStrategy;
import com.ctrip.xpipe.redis.core.route.RouteChooseStrategyFactory;
import org.springframework.stereotype.Component;

@Component
public class DefaultRouteChooseStrategyFactory implements RouteChooseStrategyFactory {
    private volatile static RouteChooseStrategy strategy;

    public DefaultRouteChooseStrategyFactory() {
    }

    public RouteChooseStrategy getRouteChooseStrategy(RouteStrategyType routeStrategyType) {
        if(strategy == null) {
            synchronized (this) {
                if(strategy == null) {
                    strategy = createRouteChooseStrategy(routeStrategyType);
                }
            }
        }
        return strategy;
    }

    private RouteChooseStrategy createRouteChooseStrategy(RouteStrategyType routeStrategyType) {
        switch (routeStrategyType) {
            case CRC32_HASH:
            default:
                return new Crc32HashRouteChooseStrategy();
        }
    }
}
