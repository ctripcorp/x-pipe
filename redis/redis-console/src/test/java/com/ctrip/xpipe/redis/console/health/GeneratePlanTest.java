package com.ctrip.xpipe.redis.console.health;

import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.google.common.collect.Lists;
import com.sun.org.apache.bcel.internal.generic.MONITORENTER;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Jan 25, 2018
 */
public class GeneratePlanTest {

    BaseSampleMonitor monitor;

    @Before
    public void beforeGeneratePlanTest() {
        monitor = new BaseSampleMonitor() {
            @Override
            protected void notifyCollectors(Sample sample) {

            }

            @Override
            protected void addRedis(BaseSamplePlan plan, String dcId, RedisMeta redisMeta) {

            }

            @Override
            protected BaseSamplePlan createPlan(String dcId, String clusterId, String shardId) {
                return new BaseSamplePlan(clusterId, shardId) {
                    @Override
                    public void addRedis(String dcId, RedisMeta redisMeta, Object initSampleResult) {
                        super.addRedis(dcId, redisMeta, initSampleResult);
                    }
                };
            }

            @Override
            public void startSample(BaseSamplePlan plan) throws SampleException {

            }
        };
    }

    @Test
    public void testGeneratePlan() {
        List<DcMeta> dcMetas = Lists.newArrayList(
                new DcMeta("SHAJQ")
                        .addCluster(new ClusterMeta("cluster1").setActiveDc("SHAJQ").setBackupDcs("SHAOY")
                                .addShard(new ShardMeta("shard1"))),
                new DcMeta("SHAOY")
                        .addCluster(new ClusterMeta("cluster1").setActiveDc("SHAJQ").setBackupDcs("SHAOY")
                        .addShard(new ShardMeta("shard1")))
        );

        Collection result = monitor.generatePlan(dcMetas);

        Assert.assertEquals(0, result.size());
    }
}
