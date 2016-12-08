package com.ctrip.xpipe.redis.console.health.hickwall;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import com.ctrip.hickwall.protocol.API;
import com.ctrip.hickwall.protocol.BinMultiDataPoint;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.health.HostPort;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.XpipeThreadFactory;

/**
 * @author marsqing
 *
 *         Dec 5, 2016 1:34:27 PM
 */
@Component
@Lazy
public class ThriftHickwallProxy implements HickwallProxy {

	private static Logger log = LoggerFactory.getLogger(ThriftHickwallProxy.class);

	@Autowired
	private ConsoleConfig config;

	private API.Iface client;

	private BlockingQueue<BinMultiDataPoint> datas;

	@PostConstruct
	private void postConstruct() throws Exception {
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
	public void writeBinMultiDataPoint(BinMultiDataPoint bmp) throws TException {
		if (!datas.offer(bmp)) {
			log.error("Hickwall queue overflow, will drop data");
		}
	}

	private HostPort parseHostPortFromConfig(String hickwallHostPort) {
		if (StringUtil.isEmpty(hickwallHostPort)) {
			throw new IllegalArgumentException("Hickwall host port is not configured");
		}

		String[] parts = hickwallHostPort.split(":");
		return new HostPort(parts[0].trim(), Integer.parseInt(parts[1].trim()));
	}

}
