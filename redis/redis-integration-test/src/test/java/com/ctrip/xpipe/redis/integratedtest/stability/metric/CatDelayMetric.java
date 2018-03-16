package com.ctrip.xpipe.redis.integratedtest.stability.metric;

import com.ctrip.xpipe.redis.integratedtest.stability.MetricLog;
import com.dianping.cat.Cat;

/**
 * @author wenchao.meng
 *         <p>
 *         Mar 15, 2018
 */
public class CatDelayMetric implements MetricLog {

    @Override
    public void log(String desc, String metricSub, long delayNanos) {
        Cat.logMetricForSum(desc, delayNanos);
    }

}
