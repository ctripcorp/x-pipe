package com.ctrip.xpipe.redis.core.meta.comparator;

import com.ctrip.xpipe.redis.core.entity.RouteMeta;

public class RouteMetaComparator extends AbstractMetaComparator<RouteMeta, RouteMeta, RouteChange> {

    public RouteMetaComparator(RouteMeta current, RouteMeta future) {
        super(current, future);
    }

    @Override
    public void compare() {

    }

    @Override
    public String idDesc() {
        return future.getRouteInfo();
    }

    public RouteMeta getCurrent() {
        return current;
    }

    public RouteMeta getFuture() {
        return future;
    }
}
