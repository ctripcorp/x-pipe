package com.ctrip.xpipe.service.metric;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import com.ctrip.framework.foundation.Foundation;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.metric.*;
import com.ctrip.xpipe.service.foundation.CtripFoundationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.hickwall.proxy.HickwallClient;
import com.ctrip.hickwall.proxy.common.DataPoint;
import com.ctrip.xpipe.utils.XpipeThreadFactory;

/**
 * @author shyin
 *
 * Jan 6, 2017
 */
public class HickwallMetric implements MetricProxy {

	private static Logger logger = LoggerFactory.getLogger(HickwallMetric.class);
	
	private HickwallConfig config = new HickwallConfig();
	
	private BlockingQueue<ArrayList<DataPoint>> datas;

	private HickwallClient client;
	
	public HickwallMetric() {
		start();
	}
	
	private void start() {
		logger.info("Hickwall proxy started.");
		
		datas = new ArrayBlockingQueue<>(config.getHickwallQueueSize());
		
		XpipeThreadFactory.create("HickwallSender", true).newThread(new Runnable() {
			@Override
			public void run() {
				tryUntilConnected();
				
				ArrayList<DataPoint> data = null;
				while (!Thread.currentThread().isInterrupted()) {
					if (data == null) {
						try {
							data = datas.take();
						} catch (InterruptedException e) {
							Thread.currentThread().interrupt();
							break;
						}
					}

					try {
						client.send(data);
						data = null;
					} catch (IOException e) {
						logger.error("Error write data to metric server{}", config.getHickwallHostPort(), e);
						tryUntilConnected();
					}catch(Exception e){
						logger.error("Error write data to metric server{}", config.getHickwallHostPort(), e);
						try {
							TimeUnit.SECONDS.sleep(5);
						} catch (InterruptedException e1) {
							Thread.currentThread().interrupt();
							break;
						}
					}
				}
			}
		}).start();
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
	public void writeBinMultiDataPoint(List<MetricData> rawData) throws MetricProxyException {
		ArrayList<DataPoint> bmp = convertToHickwallFormat(rawData);

		if (!datas.offer(bmp)) {
			logger.error("Hickwall queue overflow, will drop data");
		}
	}
	
	private ArrayList<DataPoint> convertToHickwallFormat(List<MetricData> datas) {

		ArrayList<DataPoint> dps = new ArrayList<>(datas.size());
		
		for(MetricData md : datas) {
			DataPoint dp = new DataPoint(metricName(md), (double) md.getValue(), md.getTimestampMilli() * 1000000);
			dp.setEndpoint("fx");
			dp.getTag().put("MetricSource", getLocalIP());
			dps.add(dp);
		}
		
		return dps;
	}

	private String metricName(MetricData md) {

		HostPort hostPort = md.getHostPort();
		String metricNamePrefix = toMetricNamePrefix(md);
		String metricName = metricNamePrefix + "." + hostPort.getHost() + "." + hostPort.getPort() + "." + getLocalIP();
		return metricName;
	}

	private String toMetricNamePrefix(MetricData metricData) {
		return "fx.xpipe.delay." + metricData.getClusterName() + "." + metricData.getShardName();
	}

	private String getLocalIP() {
		return Foundation.net().getHostAddress();
	}

	@Override
	public int getOrder() {
		return 0;
	}

}
