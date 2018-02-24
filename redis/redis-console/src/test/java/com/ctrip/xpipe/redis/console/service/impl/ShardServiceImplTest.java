package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.model.SetinelTbl;
import com.ctrip.xpipe.redis.console.model.ShardTbl;
import com.ctrip.xpipe.redis.console.notifier.EventType;
import com.ctrip.xpipe.redis.console.notifier.shard.ShardDeleteEvent;
import com.ctrip.xpipe.redis.console.notifier.shard.ShardEvent;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.Map;

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
        Map<Long, SetinelTbl> setinelTblMap = Maps.newHashMapWithExpectedSize(2);
        setinelTblMap.put(1L, new SetinelTbl().setSetinelAddress(sentinelAddress1));
        setinelTblMap.put(2L, new SetinelTbl().setSetinelAddress(sentinelAddress2));

        ShardEvent shardEvent = shardService.createShardDeleteEvent(clusterName, shardNames[0], shardTbl, setinelTblMap);

        Assert.assertTrue(shardEvent instanceof ShardDeleteEvent);
        Assert.assertEquals(EventType.DELETE, shardEvent.getShardEventType());

        Assert.assertEquals(clusterName, shardEvent.getClusterName());

        Assert.assertEquals(shardNames[0], shardEvent.getShardName());

        Assert.assertEquals(sentinelAddress1 + "," + sentinelAddress2, shardEvent.getShardSentinels());
        System.out.println(shardEvent);

    }
}
