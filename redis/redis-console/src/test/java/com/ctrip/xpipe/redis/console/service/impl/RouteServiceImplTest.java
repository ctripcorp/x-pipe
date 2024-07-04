package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.model.ProxyModel;
import com.ctrip.xpipe.redis.console.model.RouteModel;
import com.ctrip.xpipe.redis.console.model.consoleportal.RouteDirectionModel;
import com.ctrip.xpipe.redis.console.model.consoleportal.RouteInfoModel;
import com.ctrip.xpipe.redis.core.entity.Route;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Jul 26, 2018
 */
public class RouteServiceImplTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private RouteServiceImpl service;

    @Autowired
    private ProxyServiceImpl proxyService;

    private RouteModel route1, route2;

    private ProxyModel proxy1, proxy2, proxy3, proxy4, proxy5, proxy6;
    @Before
    public void beforeRouteServiceImplTest() {

        proxy1 = new ProxyModel().setActive(true).setDcName(dcNames[1]).setId(1).setUri("PROXYTCP://127.0.0.1:80");
        proxy2 = new ProxyModel().setActive(true).setDcName(dcNames[1]).setId(2).setUri("PROXYTCP://127.0.0.2:80");
        proxy3 = new ProxyModel().setActive(true).setDcName(dcNames[1]).setId(3).setUri("PROXYTCP://127.0.0.3:80");

        proxy4 = new ProxyModel().setActive(true).setDcName(dcNames[0]).setId(4).setUri("PROXYTLS://127.0.0.4:443");
        proxy5 = new ProxyModel().setActive(true).setDcName(dcNames[0]).setId(5).setUri("PROXYTLS://127.0.0.5:443");
        proxy6 = new ProxyModel().setActive(true).setDcName(dcNames[0]).setId(6).setUri("PROXYTLS://127.0.0.6:443");

        proxyService.addProxy(proxy1);
        proxyService.addProxy(proxy2);
        proxyService.addProxy(proxy3);

        proxyService.addProxy(proxy4);
        proxyService.addProxy(proxy5);
        proxyService.addProxy(proxy6);

        route1 = new RouteModel().setActive(true).setDstDcName(dcNames[0]).setSrcDcName(dcNames[1]).setPublic(true)
                .setClusterType(ClusterType.ONE_WAY.name())
                .setId(1).setOrgId(0L).setTag(Route.TAG_META).setSrcProxyIds("1,2, 3").setDstProxyIds("4,5,6");
        route2 = new RouteModel().setActive(false).setDstDcName(dcNames[0]).setSrcDcName(dcNames[1]).setPublic(false)
                .setId(2).setOrgId(3L).setTag(Route.TAG_META).setSrcProxyIds("1,2, 3").setDstProxyIds("4,5,6");

        service.addRoute(route1);
        service.addRoute(route2);
    }

    @Test
    public void testGetAllRoutes() {
        List<RouteModel> routes = service.getAllRoutes();
        Collections.sort(routes, new Comparator<RouteModel>() {
            @Override
            public int compare(RouteModel o1, RouteModel o2) {
                return (int) (o1.getId() - o2.getId());
            }
        });
        Assert.assertEquals(Lists.newArrayList(route1, route2), routes);
    }

    @Test
    public void testGetAllActiveRouteInfoModels(){
        List<RouteInfoModel> allActiveRouteInfos = service.getAllActiveRouteInfoModels();
        allActiveRouteInfos.forEach(routeInfoModel -> logger.info(routeInfoModel.getSrcProxies().toString()));
    }

    @Test
    public void testGetActiveRoutes() {
        List<RouteModel> routes = service.getActiveRoutes();
        Collections.sort(routes, new Comparator<RouteModel>() {
            @Override
            public int compare(RouteModel o1, RouteModel o2) {
                return (int) (o1.getId() - o2.getId());
            }
        });
        Assert.assertEquals(Lists.newArrayList(route1), routes);
    }

    @Test
    public void testUpdateRoute() {
        route1.setTag(Route.TAG_CONSOLE);
        service.updateRoute(route1);

        RouteModel route = null;
        for(RouteModel mode : service.getAllRoutes()) {
            if(mode.getId() == route1.getId()) {
                route = mode;
                break;
            }
        }
        Assert.assertEquals(Route.TAG_CONSOLE, route.getTag());
    }

    @Test
    public void testDeleteRoute() {
        service.deleteRoute(route2.getId());
        Assert.assertEquals(1, service.getAllRoutes().size());
    }

    @Test
    public void testAddRoute() {
        RouteModel route3 = new RouteModel().setActive(false).setDstDcName(dcNames[0]).setSrcDcName(dcNames[1])
                .setId(3).setOrgId(3L).setTag(Route.TAG_META).setSrcProxyIds("1,2,3").setDstProxyIds("4,5,6");
        service.addRoute(route3);
        Assert.assertEquals(3, service.getAllRoutes().size());
    }

    @Test
    public void testGetRouteInfoModelById() {
        RouteInfoModel route = service.getRouteInfoModelById(route1.getId());

        Assert.assertEquals(route1.getSrcDcName(), route.getSrcDcName());
        Assert.assertEquals(route1.getDstDcName(), route.getDstDcName());
        Assert.assertEquals(Lists.newArrayList(proxy1.getUri(), proxy2.getUri(), proxy3.getUri()), route.getSrcProxies());
        Assert.assertEquals(Lists.newArrayList(proxy4.getUri(), proxy5.getUri(), proxy6.getUri()), route.getDstProxies());
        Assert.assertEquals(route1.isPublic(), route.isPublic());
        Assert.assertEquals(null, route.getOrgName());
        Assert.assertEquals(ClusterType.ONE_WAY.name(), route.getClusterType());
    }

    @Test
    public void testGetAllActiveRouteInfoModelsByTag() {
        List<RouteInfoModel> routes = service.getAllActiveRouteInfoModelsByTag(Route.TAG_META);
        Assert.assertEquals(1, routes.size());

        routes = service.getAllActiveRouteInfoModelsByTag(Route.TAG_CONSOLE);
        Assert.assertEquals(0, routes.size());
    }

    @Test
    public void testGetAllActiveRouteInfoModelsByTagAndDirection() {
        List<RouteInfoModel> routes = service.getAllActiveRouteInfoModelsByTagAndDirection(Route.TAG_META, dcNames[1], dcNames[0]);
        Assert.assertEquals(1, routes.size());

        routes = service.getAllActiveRouteInfoModelsByTagAndDirection(Route.TAG_META, dcNames[0], dcNames[1]);
        Assert.assertEquals(0, routes.size());
    }

    @Test
    public void testGetAllRouteDirectionModelsByTag() {
        List<RouteDirectionModel> routeDirectionModes = service.getAllRouteDirectionModelsByTag(Route.TAG_META);
        Assert.assertEquals(1, routeDirectionModes.size());
        Assert.assertEquals(dcNames[1], routeDirectionModes.get(0).getSrcDcName());
        Assert.assertEquals(dcNames[0], routeDirectionModes.get(0).getDestDcName());
        Assert.assertEquals(1, routeDirectionModes.get(0).getActiveRouteNum());
        Assert.assertEquals(1, routeDirectionModes.get(0).getPublicRouteNum());
    }

    @Test
    public void testGetAllRouteInfoModelsByTagAndSrcDcName() {
        List<RouteInfoModel> routes = service.getAllActiveRouteInfoModelsByTagAndSrcDcName(Route.TAG_META, dcNames[1]);
        Assert.assertEquals(1, routes.size());

        routes = service.getAllActiveRouteInfoModelsByTagAndSrcDcName(Route.TAG_META, dcNames[0]);
        Assert.assertEquals(0, routes.size());
    }

    @Test
    public void TestAddRouteByRouteInfoModel() {
        List<String> srcProxies = Lists.newArrayList(proxy1.getUri(), proxy2.getUri());
        List<String> dstProxies = Lists.newArrayList(proxy5.getUri(), proxy6.getUri());

        RouteInfoModel routeInfoModel = new RouteInfoModel().setActive(true).setPublic(true).setDstDcName(dcNames[0])
                .setSrcDcName(dcNames[1]).setId(4).setTag(Route.TAG_META).setSrcProxies(srcProxies).setDstProxies(dstProxies);
        service.addRoute(routeInfoModel);

        Assert.assertEquals(3, service.getAllRoutes().size());
    }

    @Test
    public void testUpdateRoutes() {
        List<String> srcProxies = Lists.newArrayList(proxy1.getUri(), proxy2.getUri());
        List<String> dstProxies = Lists.newArrayList(proxy5.getUri(), proxy6.getUri());

        RouteInfoModel routeInfoModel = new RouteInfoModel().setActive(true).setPublic(false).setDstDcName(dcNames[0])
                .setSrcDcName(dcNames[1]).setId(4).setTag(Route.TAG_META).setSrcProxies(srcProxies).setDstProxies(dstProxies);

        service.addRoute(routeInfoModel);

        List<RouteInfoModel> routeInfoModels = service.getAllActiveRouteInfoModelsByTag(Route.TAG_META);
        Assert.assertEquals(2, routeInfoModels.size());
        Assert.assertEquals(true, routeInfoModels.get(0).isPublic());
        Assert.assertEquals(false, routeInfoModels.get(1).isPublic());

        routeInfoModels.get(0).setPublic(false);
        routeInfoModels.get(1).setPublic(true);

        service.updateRoutes(routeInfoModels);
        routeInfoModels = service.getAllActiveRouteInfoModelsByTag(Route.TAG_META);
        Assert.assertEquals(false, routeInfoModels.get(0).isPublic());
        Assert.assertEquals(true, routeInfoModels.get(1).isPublic());
    }
}