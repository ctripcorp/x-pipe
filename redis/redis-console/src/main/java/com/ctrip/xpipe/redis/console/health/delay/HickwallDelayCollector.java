package com.ctrip.xpipe.redis.console.health.delay;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ctrip.hickwall.protocol.BinDataPoint;
import com.ctrip.hickwall.protocol.BinMultiDataPoint;
import com.ctrip.hickwall.protocol.DataPoint;
import com.ctrip.xpipe.redis.console.health.HostPort;
import com.ctrip.xpipe.redis.console.health.hickwall.HickwallProxy;

/**
 * @author marsqing
 *
 *         Dec 2, 2016 4:39:39 PM
 */
@Component
public class HickwallDelayCollector implements DelayCollector {

	private static Logger log = LoggerFactory.getLogger(HickwallDelayCollector.class);

	@Autowired
	private HickwallProxy proxy;

	@Override
	public void collect(DelaySampleResult result) {
		String metricNamePrefix = toMetricNamePrefix(result);

		try {
			BinMultiDataPoint bmp = new BinMultiDataPoint();

			for (Entry<HostPort, Long> entry : result.getSlaveHostPort2Delay().entrySet()) {
				String metricName = metricNamePrefix + "." + entry.getKey().getHost() + "." + entry.getKey().getPort();
				addPoint(bmp, metricName, entry.getValue(), result);
			}

			HostPort masterHostPort = result.getMasterHostPort();
			addPoint(bmp, metricNamePrefix + "." + masterHostPort.getHost() + "." + masterHostPort.getPort(), result.getMasterDelayNanos(), result);

			proxy.writeBinMultiDataPoint(bmp);
		} catch (TException e) {
			log.error("Error send metrics to hickwall", e);
		}
	}

	private void addPoint(BinMultiDataPoint bmp, String metricName, long value, DelaySampleResult result) throws TException {
		DataPoint dataPoint = new DataPoint();
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

		BinDataPoint bdp = new BinDataPoint();
		TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
		byte[] data = serializer.serialize(dataPoint);
		bdp.setEncoded(data);
		bdp.setEndpoint("fx");

		bmp.addToPoints(bdp);
	}

	private String toMetricNamePrefix(DelaySampleResult result) {
		return "fx.xpipe.delay." + result.getClusterId() + "." + result.getShardId();
	}

}
