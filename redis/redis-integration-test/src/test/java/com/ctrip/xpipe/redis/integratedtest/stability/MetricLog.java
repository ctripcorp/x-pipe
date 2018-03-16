package com.ctrip.xpipe.redis.integratedtest.stability;

import com.ctrip.xpipe.redis.integratedtest.stability.metric.CatDelayMetric;
import com.ctrip.xpipe.redis.integratedtest.stability.metric.HickwallMetric;

/**
 * @author wenchao.meng
 *         <p>
 *         Mar 15, 2018
 */
public interface MetricLog {

    public static String clusterKey = "stability";

    String type = System.getProperty("metric", "hickwall");

    void log(String metric, String metricSub, long value);

    static MetricLog create() {

        if (type.equalsIgnoreCase("hickwall")) {
            return new HickwallMetric();
        }
        return new CatDelayMetric();
    }

}
