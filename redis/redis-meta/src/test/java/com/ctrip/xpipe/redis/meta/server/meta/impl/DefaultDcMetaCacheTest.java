package com.ctrip.xpipe.redis.meta.server.meta.impl;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.MetaClone;
import com.ctrip.xpipe.redis.core.meta.comparator.DcRouteMetaComparator;
import com.ctrip.xpipe.redis.core.route.RouteChooseStrategy;
import com.ctrip.xpipe.redis.core.route.RouteChooseStrategyFactory;
import com.ctrip.xpipe.redis.core.route.impl.DefaultRouteChooseStrategyFactory;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.redis.meta.server.config.UnitTestServerConfig;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Map;

import static org.mockito.Mockito.*;

/**
 * @author wenchao.meng
 *         <p>
 *         Nov 13, 2017
 */
public class DefaultDcMetaCacheTest extends AbstractMetaServerTest{

    private DefaultDcMetaCache dcMetaCache;

    @Autowired
    private RouteChooseStrategyFactory routeChooseStrategyFactory;

    @Before
    public void beforeDefaultDcMetaCacheTest(){
        dcMetaCache = new DefaultDcMetaCache();
    }

    @Test
    public void testChangeDcMetaLog(){

        //just check exception

        EventMonitor.DEFAULT.logEvent("type", getTestName());

        XpipeMeta xpipeMeta = getXpipeMeta();
        dcMetaCache.setMetaServerConfig(new UnitTestServerConfig());
        DcMeta dcMeta = (DcMeta) xpipeMeta.getDcs().values().toArray()[0];

        DcMeta future = MetaClone.clone(dcMeta);
        ClusterMeta futureCluster = (ClusterMeta) future.getClusters().values().toArray()[0];
        futureCluster.addShard(new ShardMeta().setId(randomString(5)));

        future.addCluster(new ClusterMeta().setId(randomString(10)));

        dcMetaCache.changeDcMeta(dcMeta, future, System.currentTimeMillis());

        Long clusterDbId = randomLong();
        dcMetaCache.clusterAdded(new ClusterMeta().setId("add_" + randomString(5)).setDbId(clusterDbId));
        dcMetaCache.clusterDeleted(clusterDbId);

    }

    @Test
    public void testChangeDcMetaSkip() {
        XpipeMeta xpipeMeta = getXpipeMeta();
        dcMetaCache.setMetaServerConfig(new UnitTestServerConfig());

        // check change success
        DcMeta dcMeta = (DcMeta) xpipeMeta.getDcs().values().toArray()[0];
        DcMeta future = MetaClone.clone(dcMeta);
        future.getClusters().put("mockTestClusterForChangeSuccess", new ClusterMeta());
        dcMetaCache.changeDcMeta(dcMeta, future, System.currentTimeMillis());
        Assert.assertEquals(dcMetaCache.getClusters().size(), future.getClusters().size());

        // check change skip
        dcMeta = future;
        future = MetaClone.clone(future);
        future.getClusters().put("mockTestClusterForChangeSkip", new ClusterMeta());
        dcMetaCache.changeDcMeta(dcMeta, future, System.currentTimeMillis() - 10000);
        Assert.assertNotEquals(dcMetaCache.getClusters().size(), future.getClusters().size());
    }

    @Test
    public void testChangeDcMetaAndPullOldMeta() {
        XpipeMeta xpipeMeta = getXpipeMeta();
        UnitTestServerConfig localConfig = new UnitTestServerConfig();
        localConfig.setWaitForMetaSyncDelayMilli(5000);
        dcMetaCache.setMetaServerConfig(localConfig);

        // init DcMetaManager
        DcMeta dcMeta = (DcMeta) xpipeMeta.getDcs().values().toArray()[0];
        DcMeta future = MetaClone.clone(dcMeta);
        dcMetaCache.changeDcMeta(dcMeta, future, System.currentTimeMillis() + 10000);

        // primary dc change
        dcMeta = (DcMeta) xpipeMeta.getDcs().values().toArray()[0];
        ClusterMeta clusterMeta = (ClusterMeta) dcMeta.getClusters().values().toArray()[0];
        ShardMeta shardMeta = (ShardMeta) clusterMeta.getShards().values().toArray()[0];
        String newPrimaryDc = clusterMeta.getBackupDcs().split(",")[0];
        dcMetaCache.primaryDcChanged(clusterMeta.getDbId(), shardMeta.getDbId(), newPrimaryDc);
        Assert.assertEquals(newPrimaryDc, dcMetaCache.getPrimaryDc(clusterMeta.getDbId(), shardMeta.getDbId()));

        // pull old dc meta for MGR node delay
        DcMeta oldMeta = MetaClone.clone(dcMeta);
        dcMeta = (DcMeta) xpipeMeta.getDcs().values().toArray()[0];
        dcMetaCache.changeDcMeta(dcMeta, oldMeta, System.currentTimeMillis());
        Assert.assertEquals(newPrimaryDc, dcMetaCache.getPrimaryDc(clusterMeta.getDbId(), shardMeta.getDbId()));
    }

    @Test
    public void testRouteChangeNonChange() {
        DcMeta current = getDcMeta("fra");
        DcMeta future = MetaClone.clone(current);

        Observer observer = mock(Observer.class);
        dcMetaCache.addObserver(observer);
        dcMetaCache.checkRouteChange(current, future);
        verify(observer, never()).update(any(DcRouteMetaComparator.class), any());
    }

    @Test
    public void testRouteChangeNonMetaChange() {
        DcMeta current = getDcMeta("fra");
        DcMeta future = MetaClone.clone(current);
        future.addRoute(new RouteMeta(1000).setTag(Route.TAG_CONSOLE).setSrcDc("fra").setDstDc("jq").setRouteInfo("PROXYTCP://127.0.0.1:80 PROXYTLS://127.0.0.2:443"));
        Observer observer = mock(Observer.class);
        dcMetaCache.addObserver(observer);
        dcMetaCache.checkRouteChange(current, future);
        verify(observer, never()).update(any(DcRouteMetaComparator.class), any());
    }

    @Test
    public void testRouteChangeWithMetaAdd() {
        DcMeta current = getDcMeta("fra");
        DcMeta future = MetaClone.clone(current);
        future.addRoute(new RouteMeta(1000).setTag(Route.TAG_META).setSrcDc("fra").setDstDc("jq").setRouteInfo("PROXYTCP://127.0.0.1:80 PROXYTLS://127.0.0.2:443"));
        Observer observer = mock(Observer.class);
        dcMetaCache.addObserver(observer);
        dcMetaCache.checkRouteChange(current, future);
        verify(observer, never()).update(any(DcRouteMetaComparator.class), any());
    }

    @Test
    public void testRouteChangeWithMetaRemove() {
        DcMeta current = getDcMeta("fra");
        DcMeta future = MetaClone.clone(current);
        future.getRoutes().remove(0);

        Observer observer = mock(Observer.class);
        dcMetaCache.addObserver(observer);
        dcMetaCache.checkRouteChange(current, future);
        verify(observer, times(1)).update(any(DcRouteMetaComparator.class), any());
    }

    @Test
    public void testRouteChangeWithMetaModified() {
        DcMeta current = getDcMeta("fra");
        DcMeta future = MetaClone.clone(current);
        future.getRoutes().get(0).setRouteInfo("PROXYTCP://127.0.0.1:80 PROXYTLS://127.0.0.2:443");

        Observer observer = mock(Observer.class);
        dcMetaCache.addObserver(observer);
        dcMetaCache.checkRouteChange(current, future);
        verify(observer, times(1)).update(any(DcRouteMetaComparator.class), any());
    }

    @Test
    public void testGetClusterDesignatedRoutes() {
        RouteMeta routeMeta1 = new RouteMeta().setId(1);
        RouteMeta routeMeta3 = new RouteMeta().setId(3);
        RouteMeta routeMeta4 = new RouteMeta().setId(4);
        RouteMeta routeMeta5 = new RouteMeta().setId(5);
        RouteMeta routeMeta6 = new RouteMeta().setId(6);
        RouteMeta routeMeta9 = new RouteMeta().setId(9);
        RouteMeta routeMeta10 = new RouteMeta().setId(10);

        String clusterName1 = "cluster1";
        String clusterName2 = "cluster2";
        String biClusterName1 = "bi-cluster1";
        String biClusterName2 = "bi-cluster2";

        XpipeMeta xpipeMeta = getXpipeMeta();
        UnitTestServerConfig localConfig = new UnitTestServerConfig();
        localConfig.setWaitForMetaSyncDelayMilli(5000);
        dcMetaCache.setMetaServerConfig(localConfig);
        RouteChooseStrategyFactory routeChooseStrategyFactory = new DefaultRouteChooseStrategyFactory();
        dcMetaCache.setRouteChooseStrategyFactory(routeChooseStrategyFactory);
        RouteChooseStrategyFactory.RouteStrategyType routeStrategyType =
                RouteChooseStrategyFactory.RouteStrategyType.lookup(config.getChooseRouteStrategyType());
        RouteChooseStrategy strategy = routeChooseStrategyFactory.getRouteChooseStrategy(routeStrategyType);

        // init DcMetaManager
        DcMeta dcMeta = (DcMeta) xpipeMeta.getDcs().values().toArray()[2];
        DcMeta future = MetaClone.clone(dcMeta);
        dcMetaCache.changeDcMeta(dcMeta, future, System.currentTimeMillis() + 10000);

        Map<String, RouteMeta> routes = dcMetaCache.chooseRoutes(1L);
        Assert.assertEquals(1, routes.size());
        Assert.assertEquals(strategy.choose(Lists.newArrayList(routeMeta1, routeMeta4), clusterName1).getId(), routes.get("jq").getId());

        strategy = routeChooseStrategyFactory.getRouteChooseStrategy(routeStrategyType);
        routes = dcMetaCache.chooseRoutes(2L);
        Assert.assertEquals(1, routes.size());
        Assert.assertEquals(strategy.choose(Lists.newArrayList(routeMeta3), clusterName2).getId(), routes.get("jq").getId());

        strategy = routeChooseStrategyFactory.getRouteChooseStrategy(routeStrategyType);
        routes = dcMetaCache.chooseRoutes(4L);
        Assert.assertEquals(2, routes.size());
        Assert.assertEquals(strategy.choose(Lists.newArrayList(routeMeta6), biClusterName1).getId(), routes.get("jq").getId());
        Assert.assertEquals(strategy.choose(Lists.newArrayList(routeMeta10), biClusterName1).getId(), routes.get("oy").getId());

        strategy = routeChooseStrategyFactory.getRouteChooseStrategy(routeStrategyType);
        routes = dcMetaCache.chooseRoutes(5L);
        Assert.assertEquals(2, routes.size());
        Assert.assertEquals(strategy.choose(Lists.newArrayList(routeMeta5), biClusterName2).getId(), routes.get("jq").getId());
        Assert.assertEquals(strategy.choose(Lists.newArrayList(routeMeta9), biClusterName2).getId(), routes.get("oy").getId());

    }

}
