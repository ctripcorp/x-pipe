package com.ctrip.xpipe.redis.core.meta.comparator;

import com.ctrip.xpipe.redis.core.entity.RouteMeta;

public class RouteMetaComparator extends AbstractMetaComparator<RouteMeta> {

    private RouteMeta current, future;

    public RouteMetaComparator(RouteMeta current, RouteMeta future) {
        this.current = current;
        this.future = future;
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
