package com.ctrip.xpipe.redis.integratedtest.stability.metric;

import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.metric.MetricProxyException;
import com.ctrip.xpipe.redis.integratedtest.stability.MetricLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author wenchao.meng
 *         <p>
 *         Mar 15, 2018
 */
public class HickwallMetric implements MetricLog {

    private MetricProxy metricProxy;
    protected Logger logger = LoggerFactory.getLogger(getClass());

    public HickwallMetric() {
        try {
            metricProxy = (MetricProxy) Class.forName("com.ctrip.xpipe.service.metric.HickwallMetric").newInstance();
        } catch (Exception e) {
            logger.info("[load metric proxy]{}", e);
        }

    }


    @Override
    public void log(String desc, String metricSub, long delayNanos) {

        if(metricProxy == null){
            logger.info("[log][null return]");
            return;
        }

        MetricData metricData = new MetricData(desc, "dc", clusterKey, metricSub);
        metricData.setTimestampMilli(System.currentTimeMillis());
        metricData.setValue(delayNanos);

        try {
            metricProxy.writeBinMultiDataPoint(metricData);
        } catch (MetricProxyException e) {
            logger.error("[log]", e);
        }
    }
}
