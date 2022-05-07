package com.ctrip.xpipe.redis.core.route.impl;

import com.ctrip.xpipe.redis.core.entity.RouteMeta;
import com.ctrip.xpipe.redis.core.route.RouteChooseStrategy;
import com.ctrip.xpipe.redis.core.route.RouteChooseStrategyFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

public class Crc32HashRouteChooseStrategyTest {

    private List<RouteMeta> routes;

    @Before
    public void beforeCrc32HashRouteChooseStrategyTest() {
        routes = new LinkedList<>();
        for(long i = 0; i < 100; i++) {
            routes.add(new RouteMeta().setId(i));
        }
    }

    @Test
    public void testCrc32HashRouteChooseStrategy() {
        RouteChooseStrategy strategy = new DefaultRouteChooseStrategyFactory()
                .create(RouteChooseStrategyFactory.RouteStrategyType.CRC32_HASH);

        RouteMeta first = strategy.choose(routes, "cluster");

        for(int i = 0; i < 100; i++) {
            RouteMeta next = strategy.choose(routes, "cluster");
            Assert.assertEquals(first, next);
        }
    }

}