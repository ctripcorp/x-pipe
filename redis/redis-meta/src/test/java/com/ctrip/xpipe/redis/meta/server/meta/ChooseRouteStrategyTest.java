package com.ctrip.xpipe.redis.meta.server.meta;

import com.ctrip.xpipe.redis.core.entity.RouteMeta;

import com.ctrip.xpipe.redis.meta.server.meta.impl.HashCodeChooseRouteStrategy;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

public class ChooseRouteStrategyTest {
    List<RouteMeta> routes;
    @Before
    public void chooseBefore() {
        routes = new LinkedList<>();
        for(int i = 0; i < 100; i++) {
            routes.add(new RouteMeta().setId(i));
        }
    }
    
    @Test
    public void testHashCodeChooseRouteStrategy() {
        String clusterName = "cluster";
        HashCodeChooseRouteStrategy hashCodeChooseRouteStrategy = new HashCodeChooseRouteStrategy(clusterName.hashCode());
        RouteMeta first = hashCodeChooseRouteStrategy.choose(routes);
        for(int i = 0; i < 10; i++) {
            RouteMeta next = hashCodeChooseRouteStrategy.choose(routes);
            Assert.assertEquals(first, next);
        }
        Assert.assertEquals(first, routes.get(Math.abs(clusterName.hashCode()) % routes.size()));
    }
}
