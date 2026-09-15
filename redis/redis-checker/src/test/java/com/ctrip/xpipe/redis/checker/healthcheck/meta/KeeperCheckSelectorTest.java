package com.ctrip.xpipe.redis.checker.healthcheck.meta;

import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;

public class KeeperCheckSelectorTest {

    private CheckerConfig checkerConfig;
    private MetaCache metaCache;
    private KeeperCheckSelector selector;

    @Before
    public void setUp() {
        checkerConfig = Mockito.mock(CheckerConfig.class);
        metaCache = Mockito.mock(MetaCache.class);
        selector = new KeeperCheckSelector(checkerConfig, metaCache, "active");
    }

    @Test
    public void testDisabledAndNonOneWayAreNotSelected() {
        DcMeta dc = keeperDc("active", "one_way", "active");
        Assert.assertTrue(selector.select(dc).isEmpty());

        Mockito.when(checkerConfig.isKeeperDelayCheckEnabled()).thenReturn(true);
        dc.getClusters().values().iterator().next().setType("single_dc");
        Assert.assertTrue(selector.select(dc).isEmpty());
    }

    @Test
    public void testOnlyCurrentActiveDcOwnerIsSelected() {
        Mockito.when(checkerConfig.isKeeperDelayCheckEnabled()).thenReturn(true);
        DcMeta dc = keeperDc("active", "one_way", "other");
        Assert.assertTrue(selector.select(dc).isEmpty());

        ClusterMeta cluster = dc.getClusters().values().iterator().next();
        cluster.setActiveDc("active");
        Assert.assertEquals(1, selector.select(dc).size());
    }

    @Test
    public void testSameRegionNonTfsCandidateIsSelectedButCrossRegionIsNot() {
        Mockito.when(checkerConfig.isKeeperDelayCheckEnabled()).thenReturn(true);
        DcMeta dc = keeperDc("backup", "one_way", "active");
        KeeperMeta keeper = dc.getClusters().values().iterator().next()
                .getShards().values().iterator().next().getKeepers().get(0);

        Mockito.when(metaCache.isCrossRegion("backup", "active")).thenReturn(false, true);
        Assert.assertEquals(Collections.singletonList(keeper), selector.select(dc));
        Assert.assertTrue(selector.select(dc).isEmpty());
    }

    private DcMeta keeperDc(String dcId, String type, String activeDc) {
        DcMeta dc = new DcMeta(dcId);
        ClusterMeta cluster = new ClusterMeta().setId("cluster").setType(type).setActiveDc(activeDc);
        ShardMeta shard = new ShardMeta().setId("shard");
        shard.addKeeper(new KeeperMeta().setIp("127.0.0.1").setPort(6380));
        cluster.addShard(shard);
        dc.addCluster(cluster);
        return dc;
    }
}
