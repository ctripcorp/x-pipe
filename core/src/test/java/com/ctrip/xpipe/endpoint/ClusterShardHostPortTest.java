package com.ctrip.xpipe.endpoint;

import com.ctrip.xpipe.AbstractTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author wenchao.meng
 *         <p>
 *         Aug 21, 2017
 */
public class ClusterShardHostPortTest extends AbstractTest{

    @Test
    public void testEquals(){

        ClusterShardHostPort clusterShardHostPort = new ClusterShardHostPort("cluster1", "shard1", new HostPort("127.0.0.1", 6379));
        ClusterShardHostPort clusterShardHostPort2 = new ClusterShardHostPort("cluster1", "shard1", new HostPort("127.0.0.1", 6379));

        Assert.assertTrue(clusterShardHostPort.equals(clusterShardHostPort));
        Assert.assertTrue(clusterShardHostPort.equals(clusterShardHostPort2));

        Assert.assertFalse(clusterShardHostPort.equals(null));
        ClusterShardHostPort clusterShardHostPort3 = new ClusterShardHostPort("cluster1", "shard1", new HostPort("127.0.0.1", 63791));
        Assert.assertFalse(clusterShardHostPort.equals(clusterShardHostPort3));

    }
}
