package com.ctrip.xpipe.redis.meta.server.meta.impl;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.clone.MetaCloneFacade;
import com.ctrip.xpipe.redis.core.meta.comparator.DcMetaComparator;
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

        DcMeta future = MetaCloneFacade.INSTANCE.clone(dcMeta);
        ClusterMeta futureCluster = (ClusterMeta) future.getClusters().values().toArray()[0];
        futureCluster.addShard(new ShardMeta().setId(randomString(5)));

        future.addCluster(new ClusterMeta().setId(randomString(10)));

        dcMetaCache.changeDcMeta(dcMeta, future, System.currentTimeMillis() + 1);

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
        DcMeta future = MetaCloneFacade.INSTANCE.clone(dcMeta);
        future.getClusters().put("mockTestClusterForChangeSuccess", new ClusterMeta());
        dcMetaCache.changeDcMeta(dcMeta, future, System.currentTimeMillis());
        Assert.assertEquals(dcMetaCache.getClusters().size(), future.getClusters().size());

        // check change skip
        dcMeta = future;
        future = MetaCloneFacade.INSTANCE.clone(future);
        future.getClusters().put("mockTestClusterForChangeSkip", new ClusterMeta());
        dcMetaCache.changeDcMeta(dcMeta, future, System.currentTimeMillis() - 10000);
        Assert.assertNotEquals(dcMetaCache.getClusters().size(), future.getClusters().size());
    }

    @Test
    public void testClusterModify() throws Exception {
        XpipeMeta xpipeMeta = getXpipeMeta();
        dcMetaCache.setMetaServerConfig(new UnitTestServerConfig());


        // check change success
        DcMeta dcMeta = (DcMeta) xpipeMeta.getDcs().values().toArray()[0];
        DcMeta future = MetaCloneFacade.INSTANCE.clone(dcMeta);
        future.getClusters().put("mockTestClusterForChangeSuccess", new ClusterMeta());
        dcMetaCache.changeDcMeta(dcMeta, future, System.currentTimeMillis() + 1);
        Assert.assertEquals(dcMetaCache.getClusters().size(), future.getClusters().size());
        long lastMetaModifyTime = dcMetaCache.getMetaModifyTime().get();

        sleep(1000);
        ClusterMeta cluster = dcMeta.getClusters().get("cluster1");
        ClusterMeta newCluster = MetaCloneFacade.INSTANCE.clone(cluster);
        newCluster.getShards().put("shard3", new ShardMeta().setId("shard3"));
        dcMetaCache.clusterModified(newCluster);
        Assert.assertEquals(true, dcMetaCache.getMetaModifyTime().get() > lastMetaModifyTime);
    }

    @Test
    public void testChangeDcMetaAndPullOldMeta() {
        XpipeMeta xpipeMeta = getXpipeMeta();
        UnitTestServerConfig localConfig = new UnitTestServerConfig();
        localConfig.setWaitForMetaSyncDelayMilli(5000);
        dcMetaCache.setMetaServerConfig(localConfig);

        // init DcMetaManager
        DcMeta dcMeta = (DcMeta) xpipeMeta.getDcs().values().toArray()[0];
        DcMeta future = MetaCloneFacade.INSTANCE.clone(dcMeta);
        dcMetaCache.changeDcMeta(dcMeta, future, System.currentTimeMillis() + 10000);

        // primary dc change
        dcMeta = (DcMeta) xpipeMeta.getDcs().values().toArray()[0];
        ClusterMeta clusterMeta = (ClusterMeta) dcMeta.getClusters().values().toArray()[0];
        ShardMeta shardMeta = (ShardMeta) clusterMeta.getShards().values().toArray()[0];
        String newPrimaryDc = clusterMeta.getBackupDcs().split(",")[0];
        dcMetaCache.primaryDcChanged(clusterMeta.getDbId(), shardMeta.getDbId(), newPrimaryDc);
        Assert.assertEquals(newPrimaryDc, dcMetaCache.getPrimaryDc(clusterMeta.getDbId(), shardMeta.getDbId()));

        // pull old dc meta for MGR node delay
        DcMeta oldMeta = MetaCloneFacade.INSTANCE.clone(dcMeta);
        dcMeta = (DcMeta) xpipeMeta.getDcs().values().toArray()[0];
        dcMetaCache.changeDcMeta(dcMeta, oldMeta, System.currentTimeMillis());
        Assert.assertEquals(newPrimaryDc, dcMetaCache.getPrimaryDc(clusterMeta.getDbId(), shardMeta.getDbId()));
    }

    @Test
    public void testRouteChangeNonChange() {
        DcMeta current = getDcMeta("fra");
        DcMeta future = MetaCloneFacade.INSTANCE.clone(current);

        Observer observer = mock(Observer.class);
        dcMetaCache.addObserver(observer);
        dcMetaCache.checkRouteChange(current, future);
        verify(observer, never()).update(any(DcRouteMetaComparator.class), any());
    }

    @Test
    public void testRouteChangeNonMetaChange() {
        DcMeta current = getDcMeta("fra");
        DcMeta future = MetaCloneFacade.INSTANCE.clone(current);
        future.addRoute(new RouteMeta(1000L).setTag(Route.TAG_CONSOLE).setSrcDc("fra").setDstDc("jq").setRouteInfo("PROXYTCP://127.0.0.1:80 PROXYTLS://127.0.0.2:443"));
        Observer observer = mock(Observer.class);
        dcMetaCache.addObserver(observer);
        dcMetaCache.checkRouteChange(current, future);
        verify(observer, never()).update(any(DcRouteMetaComparator.class), any());
    }

    @Test
    public void testRouteChangeWithMetaAdd() {
        DcMeta current = getDcMeta("fra");
        DcMeta future = MetaCloneFacade.INSTANCE.clone(current);
        future.addRoute(new RouteMeta(1000L).setTag(Route.TAG_META).setSrcDc("fra").setDstDc("jq").setRouteInfo("PROXYTCP://127.0.0.1:80 PROXYTLS://127.0.0.2:443"));
        Observer observer = mock(Observer.class);
        dcMetaCache.addObserver(observer);
        dcMetaCache.checkRouteChange(current, future);
        verify(observer, never()).update(any(DcRouteMetaComparator.class), any());
    }

    @Test
    public void testRouteChangeWithMetaRemove() {
        DcMeta current = getDcMeta("fra");
        DcMeta future = MetaCloneFacade.INSTANCE.clone(current);
        future.getRoutes().remove(0);

        Observer observer = mock(Observer.class);
        dcMetaCache.addObserver(observer);
        dcMetaCache.checkRouteChange(current, future);
        verify(observer, times(1)).update(any(DcRouteMetaComparator.class), any());
    }

    @Test
    public void testRouteChangeWithMetaModified() {
        DcMeta current = getDcMeta("fra");
        DcMeta future = MetaCloneFacade.INSTANCE.clone(current);
        future.getRoutes().get(0).setRouteInfo("PROXYTCP://127.0.0.1:80 PROXYTLS://127.0.0.2:443");

        Observer observer = mock(Observer.class);
        dcMetaCache.addObserver(observer);
        dcMetaCache.checkRouteChange(current, future);
        verify(observer, times(1)).update(any(DcRouteMetaComparator.class), any());
    }

    @Test
    public void testGetClusterDesignatedRoutes() {
        RouteMeta routeMeta1 = new RouteMeta().setId(1L);
        RouteMeta routeMeta3 = new RouteMeta().setId(3L);
        RouteMeta routeMeta4 = new RouteMeta().setId(4L);
        RouteMeta routeMeta5 = new RouteMeta().setId(5L);
        RouteMeta routeMeta6 = new RouteMeta().setId(6L);
        RouteMeta routeMeta9 = new RouteMeta().setId(9L);
        RouteMeta routeMeta10 = new RouteMeta().setId(10L);

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
        RouteChooseStrategy strategy = routeChooseStrategyFactory.create(routeStrategyType);

        // init DcMetaManager
        DcMeta dcMeta = (DcMeta) xpipeMeta.getDcs().values().toArray()[2];
        DcMeta future = MetaCloneFacade.INSTANCE.clone(dcMeta);
        dcMetaCache.changeDcMeta(dcMeta, future, System.currentTimeMillis() + 10000);

        RouteMeta jqRoute, oyRoute;
        jqRoute = dcMetaCache.chooseRoute(1L, "jq");
        Assert.assertEquals(strategy.choose(Lists.newArrayList(routeMeta1, routeMeta4), clusterName1).getId(), jqRoute.getId());

        strategy = routeChooseStrategyFactory.create(routeStrategyType);
        jqRoute = dcMetaCache.chooseRoute(2L, "jq");
        Assert.assertEquals(strategy.choose(Lists.newArrayList(routeMeta3), clusterName2).getId(), jqRoute.getId());

        strategy = routeChooseStrategyFactory.create(routeStrategyType);
        jqRoute = dcMetaCache.chooseRoute(4L, "jq");
        oyRoute = dcMetaCache.chooseRoute(4L, "oy");
        Assert.assertEquals(strategy.choose(Lists.newArrayList(routeMeta6), biClusterName1).getId(), jqRoute.getId());
        Assert.assertEquals(strategy.choose(Lists.newArrayList(routeMeta10), biClusterName1).getId(), oyRoute.getId());

        strategy = routeChooseStrategyFactory.create(routeStrategyType);
        jqRoute = dcMetaCache.chooseRoute(5L, "jq");
        oyRoute = dcMetaCache.chooseRoute(5L, "oy");
        Assert.assertEquals(strategy.choose(Lists.newArrayList(routeMeta5), biClusterName2).getId(), jqRoute.getId());
        Assert.assertEquals(strategy.choose(Lists.newArrayList(routeMeta9), biClusterName2).getId(), oyRoute.getId());

    }

    @Test
    public void testKeeperMigrateOnlyNums() {
        DcMeta current = (DcMeta) getXpipeMeta().getDcs().values().toArray()[0];
        DcMeta future = MetaCloneFacade.INSTANCE.clone(current);

        future.getClusters().get("cluster1").getShards().get("shard1").addKeeper(new KeeperMeta());
        future.getClusters().get("cluster-hetero1").getShards().get("shard-hetero1").getKeepers().clear();
        future.getClusters().get("cluster2").getShards().get("cluster2-shard1").getKeepers().clear();
        future.getClusters().get("cluster2").getShards().get("cluster2-shard2").getRedises().get(0).setIp("10.0.0.1");
        DcMetaComparator comparator = new DcMetaComparator(current, future);
        comparator.setShardMigrateSupport();
        comparator.compare();

        Assert.assertEquals(2, dcMetaCache.keeperMigrateOnlyNums(comparator));
    }

    @Test
    public void testKeeperMigrateOnlyNums1() {
        DcMeta current = (DcMeta) getXpipeMeta().getDcs().values().toArray()[0];
        DcMeta future = MetaCloneFacade.INSTANCE.clone(current);

        future.getClusters().get("cluster1").getShards().get("shard1").getKeepers().get(0).setIp("10.0.0.1").setActive(false);
        future.getClusters().get("cluster1").getShards().get("shard1").getKeepers().get(1).setActive(true);
        DcMetaComparator comparator = new DcMetaComparator(current, future);
        comparator.setShardMigrateSupport();
        comparator.compare();

        Assert.assertEquals(1, dcMetaCache.keeperMigrateOnlyNums(comparator));
    }

}
