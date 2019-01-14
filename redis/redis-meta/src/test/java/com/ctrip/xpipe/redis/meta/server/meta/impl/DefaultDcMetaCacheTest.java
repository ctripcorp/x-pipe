package com.ctrip.xpipe.redis.meta.server.meta.impl;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.MetaClone;
import com.ctrip.xpipe.redis.core.meta.comparator.DcRouteMetaComparator;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.*;

/**
 * @author wenchao.meng
 *         <p>
 *         Nov 13, 2017
 */
public class DefaultDcMetaCacheTest extends AbstractMetaServerTest{

    private DefaultDcMetaCache dcMetaCache;

    @Before
    public void beforeDefaultDcMetaCacheTest(){
        dcMetaCache = new DefaultDcMetaCache();
    }

    @Test
    public void testChangeDcMetaLog(){

        //just check exception

        EventMonitor.DEFAULT.logEvent("type", getTestName());

        XpipeMeta xpipeMeta = getXpipeMeta();
        DcMeta dcMeta = (DcMeta) xpipeMeta.getDcs().values().toArray()[0];

        DcMeta future = MetaClone.clone(dcMeta);
        ClusterMeta futureCluster = (ClusterMeta) future.getClusters().values().toArray()[0];
        futureCluster.addShard(new ShardMeta().setId(randomString(5)));

        future.addCluster(new ClusterMeta().setId(randomString(10)));

        dcMetaCache.changeDcMeta(dcMeta, future);

        dcMetaCache.clusterAdded(new ClusterMeta().setId("add_" + randomString(5)));
        dcMetaCache.clusterDeleted("del_" + randomString(5));

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
}
