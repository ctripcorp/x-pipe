package com.ctrip.xpipe.redis.core.meta.impl;

import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.entity.RouteMeta;
import com.ctrip.xpipe.redis.core.route.RouteChooseStrategy;
import com.ctrip.xpipe.redis.core.route.impl.Crc32HashRouteChooseStrategy;
import com.ctrip.xpipe.redis.core.route.impl.DefaultRouteChooseStrategyFactory;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

public class DefaultDcMetaManagerTest extends AbstractRedisTest {
    private DefaultDcMetaManager dcMetaManager;

    private String dc = "fra", clusterId1 = "cluster1", clusterId2 = "cluster2", biClusterId1 = "bi-cluster1", biClusterId2 = "bi-cluster2";;
    @SuppressWarnings("unused")
    private String dcBak1 = "jq", dcBak2 = "oy";

    @Before
    public void beforeDefaultFileDaoTest() throws Exception {

        dcMetaManager = (DefaultDcMetaManager) DefaultDcMetaManager.buildFromFile(dc, "file-dao-test.xml");
        add(dcMetaManager);
    }


    @Test
    public void testChooseRouteOneWay() {
        RouteMeta routeMeta1 = new RouteMeta().setId(1);
        RouteMeta routeMeta2 = new RouteMeta().setId(2);
        RouteMeta routeMeta3 = new RouteMeta().setId(3);

        RouteChooseStrategy strategy = new Crc32HashRouteChooseStrategy(clusterId1);

        Map<String, RouteMeta> chooseRoute = dcMetaManager.chooseRoute(clusterId1, DefaultRouteChooseStrategyFactory.CRC32_HASH_ROUTE_CHOOSE_STRATEGY);
        Assert.assertEquals(1, chooseRoute.size());
        Assert.assertEquals(strategy.choose(Lists.newArrayList(routeMeta1, routeMeta2)).getId(), chooseRoute.get(dcBak1).getId());

        strategy = new Crc32HashRouteChooseStrategy(clusterId2);

        chooseRoute = dcMetaManager.chooseRoute(clusterId2, DefaultRouteChooseStrategyFactory.CRC32_HASH_ROUTE_CHOOSE_STRATEGY);
        Assert.assertEquals(1, chooseRoute.size());
        Assert.assertEquals(strategy.choose(Lists.newArrayList(routeMeta3)).getId(), chooseRoute.get(dcBak1).getId());
    }

    @Test
    public void testChooseRouteBiDirection() {
        RouteMeta routeMeta1 = new RouteMeta().setId(1);
        RouteMeta routeMeta2 = new RouteMeta().setId(2);
        RouteMeta routeMeta3 = new RouteMeta().setId(3);
        RouteMeta routeMeta4 = new RouteMeta().setId(4);
        RouteMeta routeMeta5 = new RouteMeta().setId(5);
        RouteMeta routeMeta6 = new RouteMeta().setId(6);
        RouteMeta routeMeta9 = new RouteMeta().setId(9);
        RouteMeta routeMeta10 = new RouteMeta().setId(10);
        RouteChooseStrategy strategy = new Crc32HashRouteChooseStrategy(biClusterId1);

        Map<String, RouteMeta> chooseRoute = dcMetaManager.chooseRoute(biClusterId1, DefaultRouteChooseStrategyFactory.CRC32_HASH_ROUTE_CHOOSE_STRATEGY);
        Assert.assertEquals(2, chooseRoute.size());
        Assert.assertEquals(strategy.choose(Lists.newArrayList(routeMeta5)).getId(), chooseRoute.get(dcBak1).getId());
        Assert.assertEquals(strategy.choose(Lists.newArrayList(routeMeta9)).getId(), chooseRoute.get(dcBak2).getId());

        strategy = new Crc32HashRouteChooseStrategy(biClusterId2);

        chooseRoute = dcMetaManager.chooseRoute(biClusterId2, DefaultRouteChooseStrategyFactory.CRC32_HASH_ROUTE_CHOOSE_STRATEGY);
        Assert.assertEquals(2, chooseRoute.size());
        Assert.assertEquals(strategy.choose(Lists.newArrayList(routeMeta4)).getId(), chooseRoute.get(dcBak1).getId());
        Assert.assertEquals(strategy.choose(Lists.newArrayList(routeMeta10)).getId(), chooseRoute.get(dcBak2).getId());
    }


}
