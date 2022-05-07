package com.ctrip.xpipe.redis.core.meta.comparator;

import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.Route;
import com.ctrip.xpipe.redis.core.entity.RouteMeta;
import com.ctrip.xpipe.redis.core.meta.MetaClone;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DcRouteMetaComparatorTest extends AbstractComparatorTest {

    private DcRouteMetaComparator comparator;

    private DcMeta current;

    private DcMeta future;

    @Before
    public void beforeDcRouteMetaComparatorTest() {
        current = getDcMeta("fra");
        future = MetaClone.clone(current);
        Assert.assertFalse(current.getClusters().isEmpty());
    }

    @Test
    public void testCompareWithNoChanges() {
        comparator = new DcRouteMetaComparator(current, future);
        comparator.compare();

        Assert.assertTrue(comparator.getAdded().isEmpty());
        Assert.assertTrue(comparator.getRemoved().isEmpty());
        Assert.assertTrue(comparator.getMofified().isEmpty());
    }

    @Test
    public void testCompareWithRouteAdd() {
        future.addRoute(new RouteMeta().setId(1000L).setRouteInfo("PROXYTCP://127.0.0.1:80 PROXYTLS://127.0.0.7:443").setSrcDc("fra").setDstDc("jq"));
        comparator = new DcRouteMetaComparator(current, future);
        comparator.compare();

        Assert.assertTrue(comparator.getAdded().isEmpty());
        Assert.assertTrue(comparator.getRemoved().isEmpty());
        Assert.assertTrue(comparator.getMofified().isEmpty());

        future.addRoute(new RouteMeta().setId(2000L).setTag(Route.TAG_META));
        comparator = new DcRouteMetaComparator(current, future);
        comparator.compare();

        Assert.assertFalse(comparator.getAdded().isEmpty());
        Assert.assertTrue(comparator.getRemoved().isEmpty());
        Assert.assertTrue(comparator.getMofified().isEmpty());
    }

    @Test
    public void testCompareWithRouteDeleted() {
        future.getRoutes().remove(0);
        comparator = new DcRouteMetaComparator(current, future);
        comparator.compare();

        Assert.assertTrue(comparator.getAdded().isEmpty());
        Assert.assertFalse(comparator.getRemoved().isEmpty());
        Assert.assertTrue(comparator.getMofified().isEmpty());
    }

    @Test
    public void testCompareWithModified() {
        future.getRoutes().get(0).setRouteInfo("PROXYTCP://127.0.0.1:80 PROXYTLS://127.0.0.7:443");
        comparator = new DcRouteMetaComparator(current, future);
        comparator.compare();

        Assert.assertTrue(comparator.getAdded().isEmpty());
        Assert.assertTrue(comparator.getRemoved().isEmpty());
        Assert.assertFalse(comparator.getMofified().isEmpty());

        Assert.assertEquals(1, comparator.getMofified().size());
    }

    @Test
    public void testCompareWithModifiedWithPublic() {
        future.getRoutes().get(0).setIsPublic(true);
        comparator = new DcRouteMetaComparator(current, future);
        comparator.compare();

        Assert.assertTrue(comparator.getAdded().isEmpty());
        Assert.assertTrue(comparator.getRemoved().isEmpty());
        Assert.assertFalse(comparator.getMofified().isEmpty());

        Assert.assertEquals(1, comparator.getMofified().size());
    }

    @Test
    public void testCompareWithModifiedWithPublic2() {
        RouteMeta routeMeta1 = new RouteMeta().setId(4L).setRouteInfo("PROXYTCP://127.0.0.1:80 PROXYTLS://127.0.0.2:443").setIsPublic(new Boolean(true)).setTag(Route.TAG_META);
        RouteMeta routeMeta2 = new RouteMeta().setId(4L).setRouteInfo("PROXYTCP://127.0.0.1:80 PROXYTLS://127.0.0.2:443").setIsPublic(new Boolean(true)).setTag(Route.TAG_META);


        DcMeta currentMeta = new DcMeta().addRoute(routeMeta1);
        DcMeta futureMeta = new DcMeta().addRoute(routeMeta2);
        comparator = new DcRouteMetaComparator(currentMeta, futureMeta);
        comparator.compare();

        Assert.assertTrue(comparator.getAdded().isEmpty());
        Assert.assertTrue(comparator.getRemoved().isEmpty());
        Assert.assertTrue(comparator.getMofified().isEmpty());

        Assert.assertEquals(0, comparator.getMofified().size());

        futureMeta.getRoutes().get(0).setIsPublic(new Boolean(false));

        comparator = new DcRouteMetaComparator(currentMeta, futureMeta);
        comparator.compare();

        Assert.assertTrue(comparator.getAdded().isEmpty());
        Assert.assertTrue(comparator.getRemoved().isEmpty());
        Assert.assertFalse(comparator.getMofified().isEmpty());

        Assert.assertEquals(1, comparator.getMofified().size());
    }


}