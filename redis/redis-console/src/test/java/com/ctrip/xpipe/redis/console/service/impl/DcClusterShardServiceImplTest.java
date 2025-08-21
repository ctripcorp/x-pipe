package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.model.DcClusterShardTbl;
import com.ctrip.xpipe.redis.console.service.DcClusterShardService;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author chen.zhu
 * <p>
 * Jan 02, 2018
 */
public class DcClusterShardServiceImplTest extends AbstractServiceImplTest {

    @Autowired
    DcClusterShardService service;

    @Test
    public void testUpdateDcClusterShard() throws Exception {
        DcClusterShardTbl proto = service.find(dcNames[0], clusterName, shardNames[0]);
        long sentinelId = proto.getSetinelId();
        long expected = sentinelId + 1;
        proto.setSetinelId(expected);
        service.updateDcClusterShard(proto);
        Assert.assertEquals(expected, service.find(dcNames[0], clusterName, shardNames[0]).getSetinelId());
    }
}