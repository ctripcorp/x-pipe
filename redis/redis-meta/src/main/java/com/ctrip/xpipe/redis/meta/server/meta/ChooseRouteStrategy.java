package com.ctrip.xpipe.redis.meta.server.meta;

import com.ctrip.xpipe.redis.core.entity.RouteMeta;

import java.util.List;
import java.util.Random;

public interface ChooseRouteStrategy {
    RouteMeta choose(List<RouteMeta> routeMetas);
}



