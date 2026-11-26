package com.ctrip.xpipe.redis.console.service.meta.impl;

import com.ctrip.xpipe.redis.console.model.KeepercontainerTbl;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class LogicalBuKeeperPriorityOverriderTest {

    @Test
    public void testOnlyTfsContainersOfBuWithPriorityAreOverridden() {
        Map<Long, Integer> buPriorities = new HashMap<>();
        buPriorities.put(1L, 0);   // BU 1 switched to BM
        // BU 2 has NULL priority → absent from the map
        Map<Long, Integer> overrides = LogicalBuKeeperPriorityOverrider.buildOverrides(buPriorities, Arrays.asList(
                kc(10, "TFS_1", 1), kc(11, "DEFAULT", 1), kc(12, "TFS_1", 2), kc(13, "TFS_1", 0)));

        Assert.assertEquals(Collections.singletonMap(10L, 0), overrides);
    }

    @Test
    public void testApplyKeepsOtherKeepers() {
        ClusterMeta clusterMeta = new ClusterMeta("c1");
        ShardMeta shard = new ShardMeta("s1");
        clusterMeta.addShard(shard);
        KeeperMeta tfs = new KeeperMeta().setKeeperContainerId(10L).setPriority(2);
        KeeperMeta bm = new KeeperMeta().setKeeperContainerId(11L).setPriority(1);
        KeeperMeta noContainer = new KeeperMeta().setPriority(1);
        shard.addKeeper(tfs).addKeeper(bm).addKeeper(noContainer);

        LogicalBuKeeperPriorityOverrider.apply(clusterMeta, Collections.singletonMap(10L, 0));

        Assert.assertEquals(Integer.valueOf(0), tfs.getPriority());
        Assert.assertEquals(Integer.valueOf(1), bm.getPriority());
        Assert.assertEquals(Integer.valueOf(1), noContainer.getPriority());
    }

    private static KeepercontainerTbl kc(long id, String diskType, long buId) {
        return new KeepercontainerTbl().setKeepercontainerId(id).setKeepercontainerDiskType(diskType).setLogicalBuId(buId);
    }
}
