package com.ctrip.xpipe.redis.console.model;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DcClusterShardTest {

    private Logger logger = LoggerFactory.getLogger(DcClusterShardTest.class);

    private DcClusterShard dcClusterShard = new DcClusterShard("dc", "cluster", "shard");

    @Test
    public void testGetDcId() {
        Assert.assertEquals("dc", dcClusterShard.getDcId());
    }

    @Test
    public void testGetClusterId() {
        Assert.assertEquals("cluster", dcClusterShard.getClusterId());
    }

    @Test
    public void testGetShardId() {
        Assert.assertEquals("shard", dcClusterShard.getShardId());
    }

    @Test
    public void testEquals() {
        Assert.assertTrue(dcClusterShard.equals(new DcClusterShard("dc", "cluster", "shard")));
    }

    @Test
    public void testToString() {
        logger.info("{}", dcClusterShard.toString());
    }
}