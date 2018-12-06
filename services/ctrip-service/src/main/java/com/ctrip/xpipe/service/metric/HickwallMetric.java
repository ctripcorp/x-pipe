package com.ctrip.xpipe.service.metric;

import com.ctrip.framework.foundation.Foundation;
import com.ctrip.hickwall.proxy.HickwallClient;
import com.ctrip.hickwall.proxy.common.DataPoint;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.metric.MetricProxyException;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.*;

/**
 * @author shyin
 *
 * Jan 6, 2017
 */
public class HickwallMetric implements MetricProxy {

	private static Logger logger = LoggerFactory.getLogger(HickwallMetric.class);
	
	private HickwallConfig config = new HickwallConfig();
	
	private BlockingQueue<DataPoint> datas;

	private HickwallClient client;

	private ArrayList<DataPoint> dataToSend = null;

	private static final int NUM_MESSAGES_PER_SEND = 100;

	private static final int HICKWALL_SEND_INTERVAL = 2000;
	
	public HickwallMetric() {
		start();
	}
	
	private void start() {
		logger.info("Hickwall proxy started.");
		
		datas = new ArrayBlockingQueue<>(config.getHickwallQueueSize());

		ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(1,
				XpipeThreadFactory.create("HickwallSender", true));
		tryUntilConnected();

		scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {
			@Override
			protected void doRun() throws Exception {

				while(datas.size() >= NUM_MESSAGES_PER_SEND) {
					if (dataToSend == null) {
						dataToSend = new ArrayList<>();
						datas.drainTo(dataToSend, NUM_MESSAGES_PER_SEND);
					}

					try {
						client.send(dataToSend);
						dataToSend = null;
					} catch (IOException e) {
						logger.error("Error write data to metric server{}", config.getHickwallHostPort(), e);
						tryUntilConnected();
					} catch (Exception e) {
						logger.error("Error write data to metric server{}", config.getHickwallHostPort(), e);
						try {
							TimeUnit.SECONDS.sleep(10);
						} catch (InterruptedException e1) {
							Thread.currentThread().interrupt();
						}
					}
				}

			}
		}, HICKWALL_SEND_INTERVAL, HICKWALL_SEND_INTERVAL, TimeUnit.MILLISECONDS);
	}
	
	private void tryUntilConnected() {

		while(! Thread.currentThread().isInterrupted()) {

			String hickwallHostPort = config.getHickwallHostPort();
			try {
				logger.info("[tryUntilConnected][begin]{}", hickwallHostPort);
				client = new HickwallClient(hickwallHostPort);
				logger.info("[tryUntilConnected][end]{}", hickwallHostPort);
				break;
			} catch (IOException e) {
				logger.error("Error connect to metric server {}", hickwallHostPort, e);
				try {
					TimeUnit.SECONDS.sleep(5);
				} catch (InterruptedException e1) {
					Thread.currentThread().interrupt();
				}
			}
		}
	}
	
	@Override
	public void writeBinMultiDataPoint(MetricData rawData) throws MetricProxyException {
		DataPoint bmp = convertToHickwallFormat(rawData);

		if (!datas.offer(bmp)) {
			logger.error("Hickwall queue overflow, will drop data");
		}
	}
	
	private DataPoint convertToHickwallFormat(MetricData md) {

		DataPoint dp = new DataPoint(metricName(md), md.getValue(), md.getTimestampMilli() * 1000000);
		// cluster.shard.10_2_2_2_6379.10_28_142_142 (cluster.shard.redis+port.console)
		dp.setEndpoint(getEndpoint(md));
		dp.getMeta().put("measurement", String.format("fx.xpipe.%s", md.getMetricType()));
		dp.getTag().put("cluster", md.getClusterName());
		dp.getTag().put("shard", md.getShardName());
		dp.getTag().put("address", md.getHostPort().toString());
		dp.getTag().put("srcaddr", getLocalIP());
		dp.getTag().put("app", "fx");
		dp.getTag().put("dc", md.getDcName());
		addOtherTags(dp, md);
		return dp;
	}

	private String metricName(MetricData md) {

		HostPort hostPort = md.getHostPort();
		String metricNamePrefix = toMetricNamePrefix(md);
		String metricName = metricNamePrefix;
		if(hostPort != null){
			metricName += "." + hostPort.getHost() + "." + hostPort.getPort() + "." + getLocalIP();
		}
		return metricName;
	}

	private String toMetricNamePrefix(MetricData metricData) {
		return String.format("fx.xpipe.%s.%s.%s", metricData.getMetricType(), metricData.getClusterName(), metricData.getShardName());
	}

	private void addOtherTags(DataPoint dp, MetricData md) {
		if(md.getTags() != null && !md.getTags().isEmpty()) {
			for(Map.Entry<String, String> entry : md.getTags().entrySet()) {
				dp.getTag().put(entry.getKey(), entry.getValue());
			}
		}
	}

	private String getLocalIP() {
		return Foundation.net().getHostAddress();
	}

	private String getEndpoint(MetricData md) {
		String redisToPattern = getFormattedRedisAddr(md.getHostPort());
		String srcConsoleIpToPattern = getFormattedSrcAddr(getLocalIP());
		return String.format("%s.%s.%s.%s", md.getClusterName(), md.getShardName(), redisToPattern, srcConsoleIpToPattern);
	}

	@VisibleForTesting
	protected String getFormattedRedisAddr(HostPort hostPort) {
		return hostPort.getHost().replaceAll("\\.", "_") + "_" + hostPort.getPort();
	}

	@VisibleForTesting
	protected String getFormattedSrcAddr(String ipAddr) {
		return ipAddr.replaceAll("\\.", "_");
	}

	@Override
	public int getOrder() {
		return 0;
	}

}
