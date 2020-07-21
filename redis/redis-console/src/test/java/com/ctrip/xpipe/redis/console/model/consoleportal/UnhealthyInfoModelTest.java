package com.ctrip.xpipe.redis.console.model.consoleportal;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import org.junit.Assert;
import org.junit.Test;

public class UnhealthyInfoModelTest extends AbstractConsoleTest {

    @Test
    public void testMerge() {
        UnhealthyInfoModel origin = new UnhealthyInfoModel();
        UnhealthyInfoModel target = new UnhealthyInfoModel();

        origin.addUnhealthyInstance("cluster1", "jq", "shard1", new HostPort("127.0.0.1", 6379));
        target.addUnhealthyInstance("cluster2", "oy", "shard1", new HostPort("127.0.0.2", 6479));

        origin.merge(target);
        Assert.assertEquals(2, origin.getUnhealthyCluster());
        Assert.assertEquals(2, origin.getUnhealthyShard());
        Assert.assertEquals(2, origin.getUnhealthyRedis());

        target.addUnhealthyInstance("cluster1", "jq", "shard1", new HostPort("127.0.0.2", 6379));
        origin.merge(target);
        Assert.assertEquals(2, origin.getUnhealthyCluster());
        Assert.assertEquals(2, origin.getUnhealthyShard());
        Assert.assertEquals(3, origin.getUnhealthyRedis());


    }

}
