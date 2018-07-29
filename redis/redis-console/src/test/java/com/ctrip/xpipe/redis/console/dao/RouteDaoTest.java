package com.ctrip.xpipe.redis.console.dao;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.model.RouteTbl;
import com.ctrip.xpipe.redis.core.entity.Route;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

import static org.junit.Assert.*;

/**
 * @author chen.zhu
 * <p>
 * Jun 19, 2018
 */
public class RouteDaoTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private RouteDao routeDao;

    private RouteTbl proto1;

    private RouteTbl proto2;

    @Before
    public void beforeRouteDaoTest() {

        proto1 = new RouteTbl().setRouteOrgId(4L).setActive(true).setDstDcId(2L)
                .setDstProxyIds("1,2").setOptionalProxyIds("").setSrcDcId(1L).setSrcProxyIds("3,4").setTag(Route.TAG_META);
        proto2 = new RouteTbl().setRouteOrgId(3L).setActive(false).setDstDcId(2L)
                .setDstProxyIds("1,2").setOptionalProxyIds("").setSrcDcId(1L).setSrcProxyIds("3,4").setTag(Route.TAG_CONSOLE);

        routeDao.insert(proto1);
        routeDao.insert(proto2);

        RouteTbl proto3 = new RouteTbl().setRouteOrgId(3L).setActive(false).setDstDcId(2L).setDeleted(1)
                .setDstProxyIds("1,2").setOptionalProxyIds("").setSrcDcId(1L).setSrcProxyIds("3,4").setTag(Route.TAG_CONSOLE);
        routeDao.insert(proto3);
    }

    @Test
    public void testGetAllAvailableRoutes() {

        List<RouteTbl> routes = routeDao.getAllAvailableRoutes();
        Assert.assertEquals(1, routes.size());
        RouteTbl target = routes.get(0);
        Assert.assertEquals(proto1.getRouteOrgId(), target.getRouteOrgId());
        Assert.assertEquals(proto1.getTag(), target.getTag());
        Assert.assertEquals(proto1.getDstDcId(), target.getDstDcId());
        Assert.assertEquals(proto1.getDstProxyIds(), target.getDstProxyIds());
        Assert.assertEquals(proto1.getSrcDcId(), target.getSrcDcId());
        Assert.assertEquals(proto1.getSrcProxyIds(), target.getSrcProxyIds());

    }

    @Test
    public void testGetAllRoutes() {

        List<RouteTbl> routes = routeDao.getAllRoutes();
        Assert.assertEquals(2, routes.size());

    }


    @Test
    public void testDelete() {
        routeDao.delete(proto1.getId());
        Assert.assertEquals(1, routeDao.getAllRoutes().size());
    }

    @Test
    public void testUpdate() {
        proto1.setTag(Route.TAG_CONSOLE);
        routeDao.update(proto1);

        List<RouteTbl> routeTbls = routeDao.getAllRoutes();
        RouteTbl proto = null;
        for(RouteTbl route : routeTbls) {
            logger.info("[route] {}", route);
            if(route.getId() == proto1.getId()) {
                proto = route;
                break;
            }
        }
        Assert.assertNotNull(proto);
        Assert.assertEquals(Route.TAG_CONSOLE, proto.getTag());
    }

}