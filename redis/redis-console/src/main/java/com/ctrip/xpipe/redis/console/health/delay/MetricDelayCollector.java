package com.ctrip.xpipe.redis.console.health.delay;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.utils.ServicesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

/**
 * @author marsqing
 *         <p>
 *         Dec 2, 2016 4:39:39 PM
 */
@Component
public class MetricDelayCollector implements DelayCollector {

    private static Logger log = LoggerFactory.getLogger(MetricDelayCollector.class);

    private MetricProxy proxy = ServicesUtil.getMetricProxy();

    @Override
    public void collect(DelaySampleResult result) {

        try {
            List<MetricData> data = new LinkedList<>();

            for (Entry<HostPort, Long> entry : result.getSlaveHostPort2Delay().entrySet()) {
                MetricData point = getPoint(entry.getKey(), entry.getValue(), result);
                data.add(point);
            }

            HostPort masterHostPort = result.getMasterHostPort();
            if(masterHostPort != null) {
                MetricData point = getPoint(masterHostPort, result.getMasterDelayNanos(), result);
                data.add(point);
            }

            proxy.writeBinMultiDataPoint(data);
        } catch (Exception e) {
            log.error("Error send metrics to metric", e);
        }
    }

    private MetricData getPoint(HostPort hostPort, long value, DelaySampleResult result) {

        MetricData data = new MetricData(result.getClusterId(), result.getShardId());
        data.setValue(value/1000);
        data.setTimestampMilli(System.currentTimeMillis());
        data.setHostPort(hostPort);
        return data;
    }

}
