package com.ctrip.xpipe.utils.log;

import org.junit.*;

public class MdcUtilTest {

    @Test
    public void test() {

        String cluster = "cluster";
        String shard = "shard";
        MDCUtil.setClusterShard(cluster, shard);
        Assert.assertEquals(String.format("[%s.%s]", cluster, shard), MDCUtil.getClusterShard());

        shard = "cluster-shard";
        MDCUtil.setClusterShard(cluster, shard);
        Assert.assertEquals(String.format("[%s]", shard), MDCUtil.getClusterShard());

        //should not throw exception
        MDCUtil.setClusterShard(null, shard);
        Assert.assertEquals(String.format("[%s]", shard), MDCUtil.getClusterShard());

        MDCUtil.setClusterShard(cluster, null);
        Assert.assertEquals(String.format("[%s]", cluster), MDCUtil.getClusterShard());

        MDCUtil.setClusterShard(null, null);
        Assert.assertEquals(String.format("[%s]", null), MDCUtil.getClusterShard());

    }
}
