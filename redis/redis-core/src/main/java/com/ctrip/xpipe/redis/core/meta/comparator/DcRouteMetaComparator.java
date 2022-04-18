package com.ctrip.xpipe.redis.core.meta.comparator;

import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.Route;
import com.ctrip.xpipe.redis.core.entity.RouteMeta;
import com.ctrip.xpipe.utils.ObjectUtils;
import com.google.common.collect.Sets;
import org.unidal.tuple.Triple;

import java.util.Set;

public class DcRouteMetaComparator extends AbstractMetaComparator<RouteMeta> {

    private String routeTagFilter;

    private DcMeta current, future;

    public DcRouteMetaComparator(DcMeta current, DcMeta future, String routeTagFilter) {
        this.current = current;
        this.future = future;
        this.routeTagFilter = routeTagFilter;
    }

    public DcRouteMetaComparator(DcMeta current, DcMeta future) {
        this(current, future, Route.TAG_META);
    }

    @Override
    public void compare() {
        Triple<Set<RouteMeta>, Set<RouteMeta>, Set<RouteMeta>> result = getDiff(Sets.newHashSet(current.getRoutes()),
                Sets.newHashSet(future.getRoutes()));

        Set<RouteMeta> intersectionRouteMetas = filterTag(result.getMiddle());
        added = filterTag(result.getFirst());
        removed = filterTag(result.getLast());

        for(RouteMeta routeMeta : intersectionRouteMetas) {
            RouteMeta currentRouteMeta = getRouteMeta(current, routeMeta.getId());
            RouteMeta futureRouteMeta = getRouteMeta(future, routeMeta.getId());
            if(currentRouteMeta == null || futureRouteMeta == null) {
                modified.add(new RouteMetaComparator(currentRouteMeta, futureRouteMeta));
                continue;
            }
            if(!currentRouteMeta.getRouteInfo().equalsIgnoreCase(futureRouteMeta.getRouteInfo())
                    || !ObjectUtils.equals(currentRouteMeta.getIsPublic(), futureRouteMeta.getIsPublic())) {
                modified.add(new RouteMetaComparator(currentRouteMeta, futureRouteMeta));
            }
        }
    }

    private RouteMeta getRouteMeta(DcMeta dcMeta, Integer id) {
        for(RouteMeta routeMeta : dcMeta.getRoutes()) {
            if(routeMeta.getId().equals(id)) {
                return routeMeta;
            }
        }
        return null;
    }

    private Set<RouteMeta> filterTag(Set<RouteMeta> routes) {
        if(routes == null || routes.isEmpty()) {
            return routes;
        }
        return Sets.newHashSet(Sets.filter(routes, route -> {
            assert route != null;
            return routeTagFilter.equalsIgnoreCase(route.getTag());
        }));
    }

    @Override
    public String idDesc() {
        return current.getId();
    }
}
