package com.ctrip.xpipe.redis.console.dao;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.model.RouteTbl;
import com.ctrip.xpipe.redis.core.entity.Route;
import org.junit.Assert;
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

    @Test
    public void testGetAllAvailableRoutes() {
        Assert.assertEquals(0, routeDao.getAllAvailableRoutes().size());

        RouteTbl proto1 = new RouteTbl().setRouteOrgId(4L).setActive(true).setDstDcId(2L)
                .setDstProxyIds("1,2").setOptionalProxyIds("").setSrcDcId(1L).setSrcProxyIds("3,4").setTag(Route.TAG_META);
        RouteTbl proto2 = new RouteTbl().setRouteOrgId(3L).setActive(false).setDstDcId(2L)
                .setDstProxyIds("1,2").setOptionalProxyIds("").setSrcDcId(1L).setSrcProxyIds("3,4").setTag(Route.TAG_CONSOLE);

        routeDao.insert(proto1);
        routeDao.insert(proto2);

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
}