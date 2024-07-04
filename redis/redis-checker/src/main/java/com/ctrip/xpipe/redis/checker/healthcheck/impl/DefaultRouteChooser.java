package com.ctrip.xpipe.redis.checker.healthcheck.impl;

import com.ctrip.xpipe.redis.checker.healthcheck.RouteChooser;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.RouteMeta;
import com.ctrip.xpipe.redis.core.meta.clone.MetaCloneFacade;
import com.ctrip.xpipe.redis.core.route.RouteChooseStrategy;
import com.ctrip.xpipe.utils.MapUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author lishanglin
 * date 2024/7/2
 */
public class DefaultRouteChooser implements RouteChooser {

    private RouteChooseStrategy routeChooseStrategy;

    private Map<String, Map<String, Map<Integer, List<RouteMeta>>>> routesByDcClusterTypeOrg;

    public DefaultRouteChooser(RouteChooseStrategy routeChooseStrategy) {
        this.routeChooseStrategy = routeChooseStrategy;
        this.routesByDcClusterTypeOrg = new HashMap<>();
    }

    @Override
    public void updateRoutes(List<RouteMeta> routes) {
        Map<String, Map<String, Map<Integer, List<RouteMeta>>>> localRoutesByDcClusterTypeOrg = new HashMap<>();
        for (RouteMeta route: routes) {
            if (!route.isIsPublic()) continue;

            String upperDstDc = route.getDstDc().toUpperCase();
            String clusterType = route.getClusterType().toUpperCase();
            Integer orgId = route.getOrgId();

            Map<String, Map<Integer, List<RouteMeta>>> routesByClusterTypeOrg = MapUtils.getOrCreate(localRoutesByDcClusterTypeOrg, upperDstDc, HashMap::new);
            Map<Integer, List<RouteMeta>> routesByOrg = MapUtils.getOrCreate(routesByClusterTypeOrg, clusterType, HashMap::new);
            List<RouteMeta> routeList = MapUtils.getOrCreate(routesByOrg, orgId, ArrayList::new);
            routeList.add(MetaCloneFacade.INSTANCE.clone(route));
        }

        this.routesByDcClusterTypeOrg = localRoutesByDcClusterTypeOrg;
    }

    @Override
    public RouteMeta chooseRoute(String dstDc, ClusterMeta cluster) {
        String upperDstDc = dstDc.toUpperCase();
        String clusterType = cluster.getType().toUpperCase();
        Integer orgId = cluster.getOrgId();

        if (!this.routesByDcClusterTypeOrg.containsKey(upperDstDc)) return null;
        Map<String, Map<Integer, List<RouteMeta>>> routesByClusterTypeOrg = routesByDcClusterTypeOrg.get(upperDstDc);

        Map<Integer, List<RouteMeta>> routesByOrg = routesByClusterTypeOrg.containsKey(clusterType) ?
                routesByClusterTypeOrg.get(clusterType) : routesByClusterTypeOrg.get("");
        if (null == routesByOrg) return null;

        List<RouteMeta> routes = routesByOrg.containsKey(orgId) ? routesByOrg.get(orgId) : routesByOrg.get(0);
        if (null == routes) return null;

        return this.routeChooseStrategy.choose(routes, cluster.getId());
    }
}
