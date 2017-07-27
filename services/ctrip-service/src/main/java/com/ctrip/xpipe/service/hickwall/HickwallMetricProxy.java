package com.ctrip.xpipe.service.hickwall;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import com.ctrip.xpipe.metric.MetricProxyException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.hickwall.proxy.HickwallClient;
import com.ctrip.hickwall.proxy.common.DataPoint;
import com.ctrip.xpipe.metric.MetricBinMultiDataPoint;
import com.ctrip.xpipe.metric.MetricDataPoint;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.utils.XpipeThreadFactory;

/**
 * @author shyin
 *
 * Jan 6, 2017
 */
public class HickwallMetricProxy implements MetricProxy {

	private static Logger logger = LoggerFactory.getLogger(HickwallMetricProxy.class);
	
	private HickwallConfig config = new HickwallConfig();
	
	private BlockingQueue<ArrayList<DataPoint>> datas;

	private HickwallClient client;
	
	public HickwallMetricProxy() {
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
						logger.error("Error write data to hickwall server{}", config.getHickwallHostPort(), e);
						tryUntilConnected();
					}catch(Exception e){
						logger.error("Error write data to hickwall server{}", config.getHickwallHostPort(), e);
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
			try {
				client = new HickwallClient(config.getHickwallHostPort());
				logger.info("Connected to hickwall server {}", config.getHickwallHostPort());
				break;
			} catch (IOException e) {
				logger.error("Error connect to hickwall server {}", config.getHickwallHostPort(), e);
				try {
					TimeUnit.SECONDS.sleep(5);
				} catch (InterruptedException e1) {
					Thread.currentThread().interrupt();
				}
			}
		}
	}
	
	@Override
	public void writeBinMultiDataPoint(MetricBinMultiDataPoint mbmp) throws MetricProxyException {
		ArrayList<DataPoint> bmp = convertToHickwallFormat(mbmp);

		if (!datas.offer(bmp)) {
			logger.error("Hickwall queue overflow, will drop data");
		}
	}
	
	private ArrayList<DataPoint> convertToHickwallFormat(MetricBinMultiDataPoint mbmp) {
		ArrayList<DataPoint> dps = new ArrayList<>(mbmp.getPoints().size());
		
		for(MetricDataPoint mdp : mbmp.getPoints()) {
			DataPoint dp = new DataPoint(mdp.getMetric(), (double) mdp.getValue(), mdp.getTimestamp());
			dp.setEndpoint("fx");
			
			dps.add(dp);
		}
		
		return dps;
	}
	
	@Override
	public int getOrder() {
		return HIGHEST_PRECEDENCE;
	}

}
