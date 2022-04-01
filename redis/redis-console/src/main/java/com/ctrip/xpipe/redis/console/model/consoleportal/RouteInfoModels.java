package com.ctrip.xpipe.redis.console.model.consoleportal;

import java.util.List;

public class RouteInfoModels {
    private List<RouteInfoModel> routeInfoModels;

    public RouteInfoModels() {
    }

    public List<RouteInfoModel> getRouteInfoModels() {
        return routeInfoModels;
    }

    public RouteInfoModels setRouteInfoModels(List<RouteInfoModel> routeInfoModels) {
        this.routeInfoModels = routeInfoModels;
        return this;
    }
}
