package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.SentinelGroupModel;
import com.ctrip.xpipe.redis.console.model.SetinelTbl;
import com.ctrip.xpipe.redis.console.model.ShardTbl;
import com.ctrip.xpipe.redis.console.notifier.EventType;
import com.ctrip.xpipe.redis.console.notifier.shard.ShardDeleteEvent;
import com.ctrip.xpipe.redis.console.notifier.shard.ShardEvent;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author wenchao.meng
 *         <p>
 *         Jul 17, 2017
 */
public class ShardServiceImplTest extends AbstractServiceImplTest{
    @Test
    public void findOrCreateShardIfNotExist() throws Exception {
    }

    @Autowired
    private ShardServiceImpl shardService;

    @Test
    public void testFindAllByClusterName(){

        List<ShardTbl> allByClusterName = shardService.findAllByClusterName(clusterName);
        Assert.assertEquals(shardNames.length, allByClusterName.size());

        String invalid = randomString(10);
        allByClusterName = shardService.findAllByClusterName(invalid);
        Assert.assertEquals(0, allByClusterName.size());
    }

    @Test
    public void testCreateShardEvent() {
        String sentinelAddress1 = "10.8.187.27:44400,10.8.187.28:44400,10.8.107.230:44400,10.8.107.169:44400,10.8.107.77:44400";
        String sentinelAddress2 = "10.28.68.81:33355,10.28.68.82:33355,10.28.68.83:33355,10.8.107.198:33355,10.8.107.199:33355";
        ShardTbl shardTbl = shardService.find(clusterName, shardNames[0]);
        Map<Long, SentinelGroupModel> setinelTblMap = Maps.newHashMapWithExpectedSize(2);
        setinelTblMap.put(1L, new SetinelTbl().setSetinelAddress(sentinelAddress1));
        setinelTblMap.put(2L, new SetinelTbl().setSetinelAddress(sentinelAddress2));

        ShardEvent shardEvent = shardService.createShardDeleteEvent(clusterName,
                new ClusterTbl().setClusterName(clusterName).setClusterType(ClusterType.ONE_WAY.toString()), shardNames[0], shardTbl, setinelTblMap);

        assertTrue(shardEvent instanceof ShardDeleteEvent);
        Assert.assertEquals(EventType.DELETE, shardEvent.getShardEventType());

        Assert.assertEquals(clusterName, shardEvent.getClusterName());

        Assert.assertEquals(shardNames[0], shardEvent.getShardName());

        Assert.assertEquals(sentinelAddress1 + "," + sentinelAddress2, shardEvent.getShardSentinels());
        System.out.println(shardEvent);

    }

    @Test
    public void testCreateForCRDTCluster() {
        String clusterId = "test-cluster";
        String shardId = "test-shard";
        ShardEvent shardEvent = shardService.createShardDeleteEvent(clusterId,
                new ClusterTbl().setClusterName(clusterId).setClusterType(ClusterType.BI_DIRECTION.toString()), shardId, new ShardTbl(), null);
        Assert.assertNull(shardEvent);
    }

    @Test
    public void deleteShards() {
        ClusterTbl clusterTbl = mock(ClusterTbl.class);
        when(clusterTbl.getClusterName()).thenReturn(clusterName);
        when(clusterTbl.getClusterType()).thenReturn(ClusterType.BI_DIRECTION.name());
        shardService.deleteShards(clusterTbl, Lists.newArrayList(shardNames));

        assertTrue(shardService.findAllByClusterName(clusterName).isEmpty());
    }
}
