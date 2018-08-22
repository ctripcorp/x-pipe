package com.ctrip.xpipe.redis.console.health;

import com.google.common.collect.Maps;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * @author chen.zhu
 * <p>
 * Aug 21, 2018
 */
@Component
public class DefaultSampleClassfier implements SampleClassfier {

    @Override
    public <T> Map<SampleKey, Sample> getClassifiedSamples(long startNano, BaseSamplePlan<T> baseSamplePlan) {
        Map<SampleKey, Sample> samples = Maps.newHashMap();
        for(HealthCheckEndpoint endpoint : baseSamplePlan.getHostPort2SampleResult().keySet()) {
            SampleKey key = new SampleKey(startNano, endpoint.getHealthCheckTimeoutMilli());
            Sample sample = samples.get(key);
            if(sample == null) {
                sample = new Sample(System.currentTimeMillis(), startNano, baseSamplePlan, endpoint.getHealthCheckTimeoutMilli(), 0);
            }
            sample.increaseRemainingRedisCount();
            samples.put(key, sample);
        }
        return samples;
    }
}
