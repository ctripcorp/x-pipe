package com.ctrip.xpipe.redis.checker.healthcheck;

import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.RouteMeta;

import java.util.List;

/**
 * @author lishanglin
 * date 2024/7/2
 */
public interface RouteChooser {

    void updateRoutes(List<RouteMeta> routes);

    RouteMeta chooseRoute(String dstDc, ClusterMeta cluster);

}
