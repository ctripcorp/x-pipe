package com.ctrip.xpipe.redis.console.health;

import java.util.Map;

/**
 * @author chen.zhu
 * <p>
 * Aug 21, 2018
 */
public interface SampleClassfier {

    <T> Map<SampleKey, Sample> getClassifiedSamples(long startNano, BaseSamplePlan<T> baseSamplePlan);

}
