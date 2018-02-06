package com.ctrip.xpipe.redis.console.health;

import com.ctrip.xpipe.redis.console.constant.XPipeConsoleConstant;
import com.ctrip.xpipe.redis.console.health.delay.DelaySamplePlan;
import com.ctrip.xpipe.redis.console.health.delay.InstanceDelayResult;
import com.ctrip.xpipe.redis.console.health.ping.InstancePingResult;
import com.ctrip.xpipe.redis.console.health.ping.PingSamplePlan;
import com.ctrip.xpipe.redis.console.health.redisconf.diskless.DiskLessInstanceResult;
import com.ctrip.xpipe.redis.console.health.redisconf.diskless.DiskLessSamplePlan;
import com.ctrip.xpipe.redis.console.health.redisconf.version.VersionInstanceResult;
import com.ctrip.xpipe.redis.console.health.redisconf.version.VersionSamplePlan;
import com.ctrip.xpipe.redis.console.health.redismaster.InstanceRedisMasterResult;
import com.ctrip.xpipe.redis.console.health.redismaster.RedisMasterSamplePlan;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author chen.zhu
 * <p>
 * Feb 01, 2018
 */
public class BaseSamplePlanTest {

    private String dcId = "SHAJQ";

    private String clusterId = "cluster-test";

    private String shardId = "shard-test";

    private RedisMeta redis = new RedisMeta().setIp("127.0.0.1").setPort(6379)
            .setMaster(XPipeConsoleConstant.DEFAULT_ADDRESS);

    @Test
    public void isEmpty() throws Exception {
        BaseSamplePlan plan = new PingSamplePlan(clusterId, shardId);
        Assert.assertTrue(plan.isEmpty());

        plan.addRedis(dcId, redis, new InstancePingResult());
        Assert.assertFalse(plan.isEmpty());
    }

    @Test
    public void isEmpty2() throws Exception {
        BaseSamplePlan plan = new DelaySamplePlan(clusterId, shardId);
        Assert.assertTrue(plan.isEmpty());

        plan.addRedis(dcId, redis, new InstanceDelayResult(dcId, false));
        Assert.assertFalse(plan.isEmpty());
    }

    @Test
    public void isEmpty3() throws Exception {
        BaseSamplePlan plan = new VersionSamplePlan(clusterId, shardId);
        Assert.assertTrue(plan.isEmpty());

        plan.addRedis(dcId, redis, new VersionInstanceResult());
        Assert.assertFalse(plan.isEmpty());
    }

    @Test
    public void isEmpty4() throws Exception {
        BaseSamplePlan plan = new DiskLessSamplePlan(clusterId, shardId);
        Assert.assertTrue(plan.isEmpty());

        plan.addRedis(dcId, redis, new DiskLessInstanceResult());
        Assert.assertFalse(plan.isEmpty());
    }

    @Test
    public void isEmpty5() throws Exception {
        BaseSamplePlan plan = new RedisMasterSamplePlan(dcId, clusterId, shardId);
        Assert.assertTrue(plan.isEmpty());

        plan.addRedis(dcId, redis, new InstanceRedisMasterResult());
        Assert.assertFalse(plan.isEmpty());
    }

}