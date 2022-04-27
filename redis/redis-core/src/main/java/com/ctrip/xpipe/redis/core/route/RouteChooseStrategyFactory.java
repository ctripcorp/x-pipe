package com.ctrip.xpipe.redis.core.route;

public interface RouteChooseStrategyFactory {

    public enum RouteStrategyType {
        Crc32Hash ;
    }

    RouteChooseStrategy create(RouteStrategyType routeStrategyType, String clusterName);
}
