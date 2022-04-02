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
        future.addRoute(new RouteMeta(1000).setRouteInfo("PROXYTCP://127.0.0.1:80 PROXYTLS://127.0.0.7:443").setSrcDc("fra").setDstDc("jq"));
        comparator = new DcRouteMetaComparator(current, future);
        comparator.compare();

        Assert.assertTrue(comparator.getAdded().isEmpty());
        Assert.assertTrue(comparator.getRemoved().isEmpty());
        Assert.assertTrue(comparator.getMofified().isEmpty());

        future.addRoute(new RouteMeta(2000).setTag(Route.TAG_META));
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
}