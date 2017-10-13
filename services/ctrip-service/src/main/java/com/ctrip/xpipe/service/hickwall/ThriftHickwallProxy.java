package com.ctrip.xpipe.service.hickwall;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import com.ctrip.xpipe.metric.*;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.hickwall.protocol.API;
import com.ctrip.hickwall.protocol.BinDataPoint;
import com.ctrip.hickwall.protocol.BinMultiDataPoint;
import com.ctrip.hickwall.protocol.DataPoint;
import com.ctrip.xpipe.api.lifecycle.Ordered;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.XpipeThreadFactory;

/**
 * @author marsqing
 *
 *         Dec 5, 2016 1:34:27 PM
 */
public class ThriftHickwallProxy implements MetricProxy {

	private static Logger log = LoggerFactory.getLogger(ThriftHickwallProxy.class);

	private HickwallConfig config = new HickwallConfig();

	private API.Iface client;

	private BlockingQueue<BinMultiDataPoint> datas;

	public ThriftHickwallProxy() {
		start();
	}

	private void start() {
		log.info("Hickwall proxy started");

		datas = new ArrayBlockingQueue<>(config.getHickwallQueueSize());

		XpipeThreadFactory.create("HickwallSender", true).newThread(new Runnable() {

			@Override
			public void run() {
				tryUntilConnected();

				BinMultiDataPoint data = null;
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
						client.WriteBinMultiDataPoint(data);
						data = null;
					} catch (TException e) {
						log.error("Error write data to hickwall server {}", config.getHickwallHostPort(), e);
						tryUntilConnected();
					} catch (Exception e) {
						log.error("Unexpected error when write data to hickwall server {}", config.getHickwallHostPort(), e);
						try {
							TimeUnit.SECONDS.sleep(5);
						} catch (InterruptedException ie) {
							Thread.currentThread().interrupt();
							break;
						}
					}
				}
			}

		}).start();
	}

	private void tryUntilConnected() {

		while (!Thread.currentThread().isInterrupted()) {
			TTransport transport = null;
			try {
				HostPort hostPort = parseHostPortFromConfig(config.getHickwallHostPort());

				transport = new TFramedTransport(new TSocket(hostPort.getHost(), hostPort.getPort(), 2000));
				transport.open();
				TProtocol protocol = new TCompactProtocol(transport);
				client = new API.Client(protocol);
				log.info("Connected to hickwall server {}", hostPort);
				break;
			} catch (Exception e) {
				log.error("Error connect to hickwall server {}", config.getHickwallHostPort(), e);
				if (transport != null) {
					try {
						transport.close();
					} catch (Exception te) {
						// ignore
					}
				}
				try {
					TimeUnit.SECONDS.sleep(5);
				} catch (InterruptedException ie) {
					Thread.currentThread().interrupt();
				}
			}
		}
	}

	@Override
	public void writeBinMultiDataPoint(MetricBinMultiDataPoint mbmp) throws MetricProxyException {

		BinMultiDataPoint bmp = null;
		try {
			bmp = convertToThriftFormat(mbmp);
			if (!datas.offer(bmp)) {
				log.error("Hickwall queue overflow, will drop data");
			}
		} catch (TException e) {
			throw new MetricProxyException("data error:" + mbmp, e);
		}

	}

	private BinMultiDataPoint convertToThriftFormat(MetricBinMultiDataPoint mbmp) throws TException {
		BinMultiDataPoint bmp = new BinMultiDataPoint();

		for (MetricDataPoint mp : mbmp.getPoints()) {

			DataPoint dataPoint = new DataPoint();
			dataPoint.setMetric(mp.getMetric());
			dataPoint.setValue(mp.getValue());
			dataPoint.setTimestamp(mp.getTimestamp());

			dataPoint.setTags(mp.getTags());
			dataPoint.setMeta(mp.getMeta());

			BinDataPoint bdp = new BinDataPoint();
			TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
			byte[] data = serializer.serialize(dataPoint);
			bdp.setEncoded(data);
			bdp.setEndpoint("fx");

			bmp.addToPoints(bdp);
		}

		return bmp;
	}

	private HostPort parseHostPortFromConfig(String hickwallHostPort) {
		if (StringUtil.isEmpty(hickwallHostPort)) {
			throw new IllegalArgumentException("Hickwall host port is not configured");
		}

		String[] parts = hickwallHostPort.split(":");
		return new HostPort(parts[0].trim(), Integer.parseInt(parts[1].trim()));
	}

	@Override
	public int getOrder() {
		return Ordered.HIGHEST_PRECEDENCE;
	}

}
