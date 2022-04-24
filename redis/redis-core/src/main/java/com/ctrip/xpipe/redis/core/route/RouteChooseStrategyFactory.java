package com.ctrip.xpipe.redis.core.route;

import com.ctrip.xpipe.redis.core.route.impl.DefaultRouteChooseStrategyFactory;

public interface RouteChooseStrategyFactory {
    RouteChooseStrategyFactory DEFAULT = new DefaultRouteChooseStrategyFactory();

    RouteChooseStrategy createRouteStrategy(String strategyName, String clusterName);
}
