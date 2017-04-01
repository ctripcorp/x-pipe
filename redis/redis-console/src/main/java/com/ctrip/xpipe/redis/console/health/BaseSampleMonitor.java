package com.ctrip.xpipe.redis.console.health;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.utils.XpipeThreadFactory;

/**
 * @author marsqing
 *
 *         Dec 6, 2016 5:02:16 PM
 */
@SuppressWarnings("rawtypes")
public abstract class BaseSampleMonitor<T extends BaseInstanceResult> implements SampleMonitor<T>{

	protected Logger log = LoggerFactory.getLogger(getClass());

	@Autowired
	private ConsoleConfig config;

	@Autowired
	private RedisSessionManager redisSessionManager;

	protected ConcurrentMap<Long, Sample<T>> samples = new ConcurrentHashMap<>();

	protected abstract void notifyCollectors(Sample<T> sample);

	protected long recordSample(BaseSamplePlan<T> plan) {
		long nanoTime = System.nanoTime();
		samples.put(nanoTime, new Sample<>(System.currentTimeMillis(), nanoTime, plan, 2000));
		return nanoTime;
	}

	protected RedisSession findRedisSession(String host, int port) {
		return redisSessionManager.findOrCreateSession(host, port);
	}

	protected <C> void addInstanceResult(long nanoTime, String host, int port, C context) {
		Sample<T> sample = samples.get(nanoTime);
		if (sample != null) {
			sample.addInstanceResult(host, port, context);
		}
	}

	@PostConstruct
	public void scanSamples() {
		XpipeThreadFactory.create("SampleMonitor-" + getClass().getSimpleName(), true).newThread(new Runnable() {

			@Override
			public void run() {
				while (!Thread.currentThread().isInterrupted()) {
					try {
						doScan();
					} catch (Exception e) {
						log.error("Unexpected error when scan", e);
					} finally {
						try {
							Thread.sleep(config.getRedisReplicationHealthCheckInterval());
						} catch (InterruptedException e) {
							Thread.currentThread().interrupt();
							break;
						}
					}
				}
			}

			private void doScan() {
				Iterator<Entry<Long, Sample<T>>> iter = samples.entrySet().iterator();

				while (iter.hasNext()) {
					Sample<T> sample = iter.next().getValue();

					if (sample.isDone() || sample.isExpired()) {
						try {
							notifyCollectors(sample);
						} catch (Exception e) {
							log.error("Exception caught from notified collectors", e);
						}
						iter.remove();
					}
				}
			}

		}).start();
	}
}
