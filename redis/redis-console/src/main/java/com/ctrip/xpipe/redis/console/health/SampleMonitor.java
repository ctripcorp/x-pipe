package com.ctrip.xpipe.redis.console.health;

import java.util.Collection;
import java.util.List;

import com.ctrip.xpipe.redis.core.entity.DcMeta;

/**
 * @author marsqing
 *
 *         Dec 6, 2016 5:05:47 PM
 */
public interface SampleMonitor<T> {

	Collection<BaseSamplePlan<T>>  generatePlan(List<DcMeta> dcMetas);

	void startSample(BaseSamplePlan<T> plan) throws SampleException;

}
