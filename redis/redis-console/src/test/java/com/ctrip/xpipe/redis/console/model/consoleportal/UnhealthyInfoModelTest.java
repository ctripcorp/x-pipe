package com.ctrip.xpipe.redis.console.model.consoleportal;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

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

    @Test
    public void testModelSerialize() {
        UnhealthyInfoModel model = new UnhealthyInfoModel();
        model.addUnhealthyInstance("cluster1", "jq", "shard1", new HostPort("127.0.0.1", 6379), true);
        String json = Codec.DEFAULT.encode(model);
        UnhealthyInfoModel data = Codec.DEFAULT.decode(json, UnhealthyInfoModel.class);

        logger.info("[testModelSerialize] model {}", Codec.DEFAULT.encode(data));
        Assert.assertEquals(Collections.singleton("cluster1"), data.getUnhealthyClusterNames());
        Assert.assertEquals(Collections.singleton(new UnhealthyInfoModel.RedisHostPort(new HostPort("127.0.0.1", 6379), true)),
                data.getUnhealthyInstance().get("cluster1").get(new UnhealthyInfoModel.DcShard("jq", "shard1")));
    }

}
