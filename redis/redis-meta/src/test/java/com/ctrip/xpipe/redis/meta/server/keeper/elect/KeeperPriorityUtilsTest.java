package com.ctrip.xpipe.redis.meta.server.keeper.elect;

import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import org.junit.Assert;
import org.junit.Test;

public class KeeperPriorityUtilsTest {

    @Test
    public void normalizeNullPriorityToDefault() {
        ShardMeta shardMeta = shardWithKeepers(
                keeper(6000, null),
                keeper(6001, 0));

        KeeperPriorityUtils.normalizeShardKeepers("c1", shardMeta);

        Assert.assertEquals(Integer.valueOf(KeeperPriorityUtils.DEFAULT_PRIORITY),
                shardMeta.getKeepers().get(0).getPriority());
        Assert.assertEquals(Integer.valueOf(0), shardMeta.getKeepers().get(1).getPriority());
    }

    @Test
    public void normalizeAllZeroToDefault() {
        ShardMeta shardMeta = shardWithKeepers(
                keeper(6000, 0),
                keeper(6001, 0));

        KeeperPriorityUtils.normalizeShardKeepers("c1", shardMeta);

        Assert.assertEquals(Integer.valueOf(KeeperPriorityUtils.DEFAULT_PRIORITY),
                shardMeta.getKeepers().get(0).getPriority());
        Assert.assertEquals(Integer.valueOf(KeeperPriorityUtils.DEFAULT_PRIORITY),
                shardMeta.getKeepers().get(1).getPriority());
    }

    @Test
    public void normalizePreservesExplicitZeroWhenSiblingPositive() {
        ShardMeta shardMeta = shardWithKeepers(
                keeper(6000, 0),
                keeper(6001, 2));

        KeeperPriorityUtils.normalizeShardKeepers("c1", shardMeta);

        Assert.assertEquals(Integer.valueOf(0), shardMeta.getKeepers().get(0).getPriority());
        Assert.assertEquals(Integer.valueOf(2), shardMeta.getKeepers().get(1).getPriority());
    }

    @Test
    public void normalizeDcMetaCoversAllShards() {
        DcMeta dcMeta = new DcMeta().setId("jq");
        ClusterMeta clusterMeta = new ClusterMeta().setId("cluster1");
        ShardMeta shardMeta = shardWithKeepers(keeper(6000, null));
        shardMeta.setId("shard1");
        clusterMeta.addShard(shardMeta);
        dcMeta.addCluster(clusterMeta);

        KeeperPriorityUtils.normalizeDcMeta(dcMeta);

        Assert.assertEquals(Integer.valueOf(KeeperPriorityUtils.DEFAULT_PRIORITY),
                shardMeta.getKeepers().get(0).getPriority());
    }

    private static ShardMeta shardWithKeepers(KeeperMeta... keepers) {
        ShardMeta shardMeta = new ShardMeta().setId("shard1");
        for (KeeperMeta keeperMeta : keepers) {
            shardMeta.addKeeper(keeperMeta);
        }
        return shardMeta;
    }

    private static KeeperMeta keeper(int port, Integer priority) {
        KeeperMeta keeperMeta = new KeeperMeta();
        keeperMeta.setIp("127.0.0.1");
        keeperMeta.setPort(port);
        keeperMeta.setPriority(priority);
        return keeperMeta;
    }
}
