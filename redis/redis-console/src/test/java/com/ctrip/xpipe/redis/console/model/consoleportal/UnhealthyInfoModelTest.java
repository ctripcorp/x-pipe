package com.ctrip.xpipe.redis.console.model.consoleportal;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

public class UnhealthyInfoModelTest extends AbstractConsoleTest {

    @Test
    public void testMerge() {
        UnhealthyInfoModel origin = new UnhealthyInfoModel();
        UnhealthyInfoModel target = new UnhealthyInfoModel();

        origin.addUnhealthyInstance("cluster1", "jq", "shard1", new HostPort("127.0.0.1", 6379), true);
        target.addUnhealthyInstance("cluster2", "oy", "shard1", new HostPort("127.0.0.2", 6479), false);

        origin.merge(target);
        Assert.assertEquals(2, origin.getUnhealthyCluster());
        Assert.assertEquals(2, origin.getUnhealthyShard());
        Assert.assertEquals(2, origin.getUnhealthyRedis());

        target.addUnhealthyInstance("cluster1", "jq", "shard1", new HostPort("127.0.0.2", 6379), false);
        origin.merge(target);
        Assert.assertEquals(2, origin.getUnhealthyCluster());
        Assert.assertEquals(2, origin.getUnhealthyShard());
        Assert.assertEquals(3, origin.getUnhealthyRedis());
        Assert.assertEquals(Sets.newHashSet(new UnhealthyInfoModel.RedisHostPort(new HostPort("127.0.0.1", 6379), true)
                , new UnhealthyInfoModel.RedisHostPort(new HostPort("127.0.0.2", 6379), false)),
                origin.getUnhealthyInstance().get("cluster1").get(new UnhealthyInfoModel.DcShard("jq", "shard1")));
    }

}
