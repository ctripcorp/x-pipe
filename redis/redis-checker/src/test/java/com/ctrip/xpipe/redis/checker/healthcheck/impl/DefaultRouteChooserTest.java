package com.ctrip.xpipe.redis.checker.healthcheck.impl;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.RouteMeta;
import com.ctrip.xpipe.redis.core.route.RouteChooseStrategy;
import com.ctrip.xpipe.redis.core.route.RouteChooseStrategyFactory;
import com.ctrip.xpipe.redis.core.route.impl.DefaultRouteChooseStrategyFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.ctrip.xpipe.redis.core.entity.Route.TAG_CONSOLE;

/**
 * @author lishanglin
 * date 2024/7/3
 */
public class DefaultRouteChooserTest extends AbstractCheckerTest {

    private RouteChooseStrategy routeChooseStrategy;

    private DefaultRouteChooser routeChooser;

    private List<RouteMeta> routes;

    private String srcDc = "fra";

    private String dstDc = "jq";

    @Before
    public void setupDefaultRouteChooserTest() {
        this.routeChooseStrategy = (new DefaultRouteChooseStrategyFactory()).create(RouteChooseStrategyFactory.RouteStrategyType.CRC32_HASH);
        this.routeChooser = new DefaultRouteChooser(routeChooseStrategy);
        this.routes = new ArrayList<RouteMeta>() {{
            add(new RouteMeta(1L).setSrcDc(srcDc).setDstDc(dstDc).setClusterType("").setOrgId(0).setIsPublic(true).setTag(TAG_CONSOLE).setRouteInfo(""));
            add(new RouteMeta(2L).setSrcDc(srcDc).setDstDc(dstDc).setClusterType("").setOrgId(0).setIsPublic(false).setTag(TAG_CONSOLE).setRouteInfo(""));
            add(new RouteMeta(3L).setSrcDc(srcDc).setDstDc(dstDc).setClusterType("").setOrgId(1).setIsPublic(true).setTag(TAG_CONSOLE).setRouteInfo(""));
            add(new RouteMeta(4L).setSrcDc(srcDc).setDstDc(dstDc).setClusterType(ClusterType.BI_DIRECTION.name()).setOrgId(0).setIsPublic(true).setTag(TAG_CONSOLE).setRouteInfo(""));
            add(new RouteMeta(5L).setSrcDc(srcDc).setDstDc(dstDc).setClusterType(ClusterType.BI_DIRECTION.name()).setOrgId(0).setIsPublic(false).setTag(TAG_CONSOLE).setRouteInfo(""));
            add(new RouteMeta(6L).setSrcDc(srcDc).setDstDc(dstDc).setClusterType(ClusterType.BI_DIRECTION.name()).setOrgId(1).setIsPublic(true).setTag(TAG_CONSOLE).setRouteInfo(""));
            add(new RouteMeta(7L).setSrcDc(srcDc).setDstDc(dstDc).setClusterType(ClusterType.BI_DIRECTION.name()).setOrgId(1).setIsPublic(false).setTag(TAG_CONSOLE).setRouteInfo(""));
        }};
    }

    @Test
    public void testRouteChoose() {
        routeChooser.updateRoutes(routes);
        ClusterMeta cluster = new ClusterMeta("cluster1").setOrgId(1).setType(ClusterType.BI_DIRECTION.name());

        // cluster type + orgId
        RouteMeta route = routeChooser.chooseRoute(dstDc, cluster);
        Assert.assertEquals(6L, route.getId().longValue());

        // cluster type + default org
        cluster.setOrgId(2);
        route = routeChooser.chooseRoute(dstDc, cluster);
        Assert.assertEquals(4L, route.getId().longValue());

        // default cluster type + default orgId
        cluster.setType(ClusterType.ONE_WAY.name());
        route = routeChooser.chooseRoute(dstDc, cluster);
        Assert.assertEquals(1L, route.getId().longValue());

        // default cluster type + org id
        cluster.setOrgId(1);
        route = routeChooser.chooseRoute(dstDc, cluster);
        Assert.assertEquals(3L, route.getId().longValue());

        // no result
        route = routeChooser.chooseRoute("otherDc", cluster);
        Assert.assertNull(route);
    }

}
