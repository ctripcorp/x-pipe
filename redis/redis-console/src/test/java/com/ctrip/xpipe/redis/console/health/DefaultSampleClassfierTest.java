package com.ctrip.xpipe.redis.console.health;

import com.ctrip.xpipe.redis.console.AbstractConsoleH2DbTest;
import com.ctrip.xpipe.redis.console.health.ping.InstancePingResult;
import com.ctrip.xpipe.redis.console.health.ping.PingSamplePlan;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

/**
 * @author chen.zhu
 * <p>
 * Aug 22, 2018
 */
public class DefaultSampleClassfierTest extends AbstractConsoleH2DbTest {

    private SampleClassfier classfier = new DefaultSampleClassfier();

    private BaseSamplePlan baseSamplePlan;

    private String clusterId = "cluster", shardId = "shard", dcId = "SHAJQ";

    private HealthCheckEndpointManager healthCheckEndpointManager = new DefaultHealthCheckEndpointManager();

    @Before
    public void beforeDefaultSampleClassfierTest() {
        baseSamplePlan = new PingSamplePlan(clusterId, shardId);
    }

    @Test
    public void testGetClassifiedSamples() {
        PingSamplePlan plan = (PingSamplePlan) baseSamplePlan;
        int localRedisCount = 3, proxyedRedisCount = 5;
        for(int i = 0; i < localRedisCount; i++) {
            plan.addRedis(dcId, newDefaultHealthCheckEndpoint("localhost", randomPort()), new InstancePingResult());
        }
        for(int i = 0; i < proxyedRedisCount; i++) {
            plan.addRedis(dcId, newProxyedHealthCheckEndpoint("localhost", randomPort()), new InstancePingResult());
        }

        long startNano = System.nanoTime();

        Map<SampleKey, Sample> samples = classfier.getClassifiedSamples(startNano, plan);

        Assert.assertEquals(2, samples.size());

        Assert.assertNotNull(samples.get(new SampleKey(startNano, 1500)));

        Assert.assertNotNull(samples.get(new SampleKey(startNano, 30 * 1000)));

        Assert.assertEquals(localRedisCount, samples.get(new SampleKey(startNano, 1500)).getRemainingRedisCount());

        Assert.assertEquals(proxyedRedisCount, samples.get(new SampleKey(startNano, 30 * 1000)).getRemainingRedisCount());
    }

}