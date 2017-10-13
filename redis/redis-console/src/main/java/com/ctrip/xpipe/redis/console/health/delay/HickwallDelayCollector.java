package com.ctrip.xpipe.redis.console.health.delay;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.ctrip.xpipe.metric.HostPort;
import com.ctrip.xpipe.metric.MetricBinMultiDataPoint;
import com.ctrip.xpipe.metric.MetricDataPoint;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.utils.ServicesUtil;

/**
 * @author marsqing
 *
 *         Dec 2, 2016 4:39:39 PM
 */
@Component
public class HickwallDelayCollector implements DelayCollector {

	private static Logger log = LoggerFactory.getLogger(HickwallDelayCollector.class);

	private MetricProxy proxy = ServicesUtil.getMetricProxy();

	@Override
	public void collect(DelaySampleResult result) {
		String metricNamePrefix = toMetricNamePrefix(result);

		try {
			MetricBinMultiDataPoint bmp = new MetricBinMultiDataPoint();

			for (Entry<HostPort, Long> entry : result.getSlaveHostPort2Delay().entrySet()) {
				String metricName = metricNamePrefix + "." + entry.getKey().getHost() + "." + entry.getKey().getPort();
				addPoint(bmp, metricName, entry.getValue(), result);
			}

			HostPort masterHostPort = result.getMasterHostPort();
			addPoint(bmp, metricNamePrefix + "." + masterHostPort.getHost() + "." + masterHostPort.getPort(), result.getMasterDelayNanos(), result);

			proxy.writeBinMultiDataPoint(bmp);
		} catch (Exception e) {
			log.error("Error send metrics to hickwall", e);
		}
	}

	private void addPoint(MetricBinMultiDataPoint bmp, String metricName, long value, DelaySampleResult result) {
		MetricDataPoint dataPoint = new MetricDataPoint();
		dataPoint.setMetric(metricName);
		dataPoint.setValue(value / 1000);
		dataPoint.setTimestamp(System.currentTimeMillis() * 1000000);

		Map<String, String> tags = new HashMap<>();
		tags.put("cluster", result.getClusterId());
		tags.put("shard", result.getShardId());
		dataPoint.setTags(tags);

		Map<String, String> meta = new HashMap<>();
		meta.put("stype", "fx");
		meta.put("dtype", "float64");
		meta.put("interval", "15s"); // one dot every 15 seconds
		dataPoint.setMeta(meta);

		bmp.addToPoints(dataPoint);
	}

	private String toMetricNamePrefix(DelaySampleResult result) {
		return "fx.xpipe.delay." + result.getClusterId() + "." + result.getShardId();
	}

}
