package com.ctrip.xpipe.redis.core.route;

import com.ctrip.xpipe.redis.core.entity.RouteMeta;

import java.util.List;

public interface RouteChooseStrategy {
    RouteMeta choose(List<RouteMeta> routeMetas);
}
