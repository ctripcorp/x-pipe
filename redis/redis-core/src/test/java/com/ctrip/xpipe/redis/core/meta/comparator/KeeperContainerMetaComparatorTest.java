package com.ctrip.xpipe.redis.core.meta.comparator;

import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.MetaClone;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class KeeperContainerMetaComparatorTest extends AbstractComparatorTest {

    private DcMeta current;
    private DcMeta future;


    @Before
    public void init() {
        current = getDc();
        future = MetaClone.clone(current);
    }

    @Test
    public void testEquals() {
        KeeperContainerMetaComparator comparator = new KeeperContainerMetaComparator(current, future);
        comparator.compare();

        Assert.assertEquals(0, comparator.getAdded().size());
        Assert.assertEquals(0, comparator.getRemoved().size());
        Assert.assertEquals(0, comparator.getMofified().size());
    }

    @Test
    public void testAddOrRemoveKeeperContainer() {
        KeeperContainerMeta newKeeperContainer = generateKeeperContainerMeta(4L, "127.0.0.4", 7080, future);
        future.addKeeperContainer(newKeeperContainer);
        future.addCluster(generateClusterMeta().addShard(generateShardMeta().addKeeper(generateKeeperMeta(newKeeperContainer))));

        KeeperContainerMetaComparator comparator = new KeeperContainerMetaComparator(current, future);
        comparator.compare();

        Assert.assertEquals(1, comparator.getAdded().size());
        Assert.assertEquals(0, comparator.getRemoved().size());
        Assert.assertEquals(0, comparator.getMofified().size());

        comparator = new KeeperContainerMetaComparator(future, current);
        comparator.compare();

        Assert.assertEquals(0, comparator.getAdded().size());
        Assert.assertEquals(1, comparator.getRemoved().size());
        Assert.assertEquals(0, comparator.getMofified().size());

    }

    @Test
    public void testWhenKeeperChange() {
       ClusterMeta cluster1 = current.getClusters().get("cluster1");
       ClusterMeta newCluster1 = MetaClone.clone(cluster1);
       newCluster1.getShards().get("shard1").addKeeper(new KeeperMeta().setKeeperContainerId(3L).setIp("127.0.0.3").setPort(randomPort()));
       future.addCluster(newCluster1);

        KeeperContainerMetaComparator comparator = new KeeperContainerMetaComparator(current, future);
        comparator.compare();

        Assert.assertEquals(1, comparator.getAdded().size());
        Assert.assertEquals(0, comparator.getRemoved().size());
        Assert.assertEquals(0, comparator.getMofified().size());

        comparator = new KeeperContainerMetaComparator(future, current);
        comparator.compare();

        Assert.assertEquals(0, comparator.getAdded().size());
        Assert.assertEquals(1, comparator.getRemoved().size());
        Assert.assertEquals(0, comparator.getMofified().size());

    }

    protected KeeperContainerMeta generateKeeperContainerMeta(long id, String ip, int port, DcMeta parent) {
        return new KeeperContainerMeta().setId(id).setIp(ip).setPort(port).setParent(parent);
    }

    protected ClusterMeta generateClusterMeta() {
        return new ClusterMeta().setId(randomString()).setDbId(Math.abs(randomLong()));
    }

    protected KeeperMeta generateKeeperMeta(KeeperContainerMeta keeperContainer) {
        return new KeeperMeta().setIp(keeperContainer.getIp()).setKeeperContainerId(keeperContainer.getId())
                .setPort(randomPort()).setActive(false);
    }

    protected ShardMeta generateShardMeta() {
        String shardName = randomString();
        return new ShardMeta().setId(shardName).setSentinelId(randomLong()).setSentinelMonitorName(shardName);
    }


    @Override
    protected String getXpipeMetaConfigFile() {
        return "KeeperContainerComparator.xml";
    }
}