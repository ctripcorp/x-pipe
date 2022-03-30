package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.model.RouteModel;
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

    private RouteModel route1, route2;

    @Before
    public void beforeRouteServiceImplTest() {
        route1 = new RouteModel().setActive(true).setDstDcName(dcNames[0]).setSrcDcName(dcNames[1])
                .setId(1).setOrgId(0L).setTag(Route.TAG_META).setSrcProxyIds("1,2,3").setDstProxyIds("4,5,6");
        route2 = new RouteModel().setActive(false).setDstDcName(dcNames[0]).setSrcDcName(dcNames[1])
                .setId(2).setOrgId(3L).setTag(Route.TAG_META).setSrcProxyIds("1,2,3").setDstProxyIds("4,5,6");

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
    public void testGetAllRouteInfos(){
        List<RouteInfoModel> allActiveRouteInfos = service.getAllActiveRouteInfos();
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
}