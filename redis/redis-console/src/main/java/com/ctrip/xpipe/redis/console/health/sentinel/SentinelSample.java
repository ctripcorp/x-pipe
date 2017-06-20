package com.ctrip.xpipe.redis.console.health.sentinel;

import com.ctrip.xpipe.metric.HostPort;
import com.ctrip.xpipe.redis.console.health.BaseInstanceResult;
import com.ctrip.xpipe.redis.console.health.BaseSamplePlan;
import com.ctrip.xpipe.redis.console.health.Sample;

import java.util.HashSet;
import java.util.Set;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 19, 2017
 */
public class SentinelSample extends Sample<InstanceSentinelResult>{

    public SentinelSample(long startTime,
                          long startNanoTime,
                          BaseSamplePlan<InstanceSentinelResult> samplePlan,
                          int expireDelayMillis) {
        super(startTime, startNanoTime, samplePlan, expireDelayMillis);
    }



    public <C> void addInstanceSuccess(String host, int port, C context) {

        InstanceSentinelResult instanceResult = samplePlan.findInstanceResult(new HostPort(host, port));

        if (instanceResult != null && !instanceResult.isDone()) {
            instanceResult.success(System.nanoTime(), (SentinelHello) context);
            //wait until timeout
            //remainingRedisCount.decrementAndGet();
        }
    }


    public Set<SentinelHello> getHellos() {

        Set<SentinelHello> hellos = new HashSet<>();

        samplePlan.getHostPort2SampleResult().forEach((hostPort, result) -> {
            hellos.addAll(result.getHellos());

        });

        return hellos;
    }

}
