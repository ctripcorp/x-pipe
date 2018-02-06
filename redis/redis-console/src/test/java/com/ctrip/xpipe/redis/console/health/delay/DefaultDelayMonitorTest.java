package com.ctrip.xpipe.redis.console.health.delay;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.health.HealthChecker;
import com.ctrip.xpipe.redis.console.health.Sample;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.spring.AbstractProfile;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * @author wenchao.meng
 *         <p>
 *         Dec 29, 2017
 */
public class DefaultDelayMonitorTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private DefaultDelayMonitor delayMonitor;

    @BeforeClass
    public static void beforeDefaultDelayMonitorTest() {
        System.setProperty(HealthChecker.ENABLED, "true");
        System.setProperty(AbstractProfile.PROFILE_KEY, AbstractProfile.PROFILE_NAME_PRODUCTION);
    }

    @Test
    public void testTimecost() throws InterruptedException, IOException {

        logger.info("{}", delayMonitor);

        int clusterCount = 500;
        int shardCountEach = 10;

        int port1 = 10000, port2 = 20000, port3 = 30000, port4 = 40000;

        Map<Long, Sample<InstanceDelayResult>> samples = new ConcurrentHashMap<>();

        for (int i = 0; i < clusterCount; i++) {
            for (int j = 0; j < shardCountEach; j++) {

                DelaySamplePlan delaySamplePlan = new DelaySamplePlan("cluster" + i, "shard" + j);
                delaySamplePlan.addRedis("jq", new RedisMeta().setIp("127.0.0.1").setPort(++port1).setMaster(null));
                delaySamplePlan.addRedis("jq", new RedisMeta().setIp("127.0.0.1").setPort(++port2).setMaster("127"));
                delaySamplePlan.addRedis("oy", new RedisMeta().setIp("127.0.0.1").setPort(++port3).setMaster("127"));
                delaySamplePlan.addRedis("oy", new RedisMeta().setIp("127.0.0.1").setPort(++port4).setMaster("127"));


                long nanoTime = System.nanoTime();
                Sample<InstanceDelayResult> sample = new Sample<>(System.currentTimeMillis(), nanoTime, delaySamplePlan, 1500);
                sample.addInstanceSuccess("127.0.0.1", port1, null);
                sample.addInstanceSuccess("127.0.0.1", port2, null);
                sample.addInstanceSuccess("127.0.0.1", port3, null);
                sample.addInstanceSuccess("127.0.0.1", port4, null);
                samples.put(System.nanoTime(), sample);

            }
        }


        long begin = System.currentTimeMillis();
        delayMonitor.setSamples(samples);
        long end = System.currentTimeMillis();
        TimeUnit.SECONDS.sleep(10);
//        logger.info("[cost]{} ms", end - begin);

    }


    @Test
    public void avoidNullPointerException() {
        DelaySampleResult sampleResult = new DelaySampleResult(System.currentTimeMillis(), "cluster", "shard");
        sampleResult.addSlaveDelayNanos(new HostPort("127.0.0.1", 6379), System.nanoTime());
        sampleResult.addSlaveDelayNanos(new HostPort("127.0.0.1", 6380), System.nanoTime());

        try {
            delayMonitor.getDelayCollectors().forEach(collector -> collector.collect(sampleResult));
        } catch (Exception e) {
            Assert.assertFalse(e instanceof NullPointerException);
        }
    }
}
