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
                    switch (routeStrategyType) {
                        case CRC32_HASH:
                        default:
                            strategy = new Crc32HashRouteChooseStrategy();
                    }
                }
            }
        }
        return strategy;
    }
}
