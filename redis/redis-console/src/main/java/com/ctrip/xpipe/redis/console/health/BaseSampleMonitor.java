package com.ctrip.xpipe.redis.console.health;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.PostConstruct;

import com.ctrip.xpipe.metric.HostPort;
import com.ctrip.xpipe.redis.console.health.ping.InstancePingResult;
import com.ctrip.xpipe.redis.console.health.ping.PingSamplePlan;
import com.ctrip.xpipe.redis.console.health.redisconf.InstanceRedisConfResult;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import org.unidal.tuple.Pair;

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
		samples.put(nanoTime, createSample(nanoTime, plan));
		return nanoTime;
	}

	protected Sample<T> createSample(long nanoTime, BaseSamplePlan<T> plan){

		return new Sample<>(System.currentTimeMillis(), nanoTime, plan, 1500);
	}

	protected RedisSession findRedisSession(HostPort hostPort) {
		return redisSessionManager.findOrCreateSession(hostPort.getHost(), hostPort.getPort());
	}

	protected RedisSession findRedisSession(String host, int port) {
		return redisSessionManager.findOrCreateSession(host, port);
	}


	protected <C> void addInstanceSuccess(long nanoTime, HostPort hostPort, C context) {
		addInstanceSuccess(nanoTime, hostPort.getHost(), hostPort.getPort(), context);
	}

	protected <C> void addInstanceSuccess(long nanoTime, String host, int port, C context) {
		Sample<T> sample = samples.get(nanoTime);
		if (sample != null) {
			sample.addInstanceSuccess(host, port, context);
		}
	}

	protected <C> void addInstanceFail(long nanoTime, HostPort hostPort, Throwable th) {
		addInstanceFail(nanoTime, hostPort.getHost(), hostPort.getPort(), th);
	}

	protected <C> void addInstanceFail(long nanoTime, String host, int port, Throwable th) {
		Sample<T> sample = samples.get(nanoTime);
		if (sample != null) {
			sample.addInstanceFail(host, port, th);
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

	@Override
	public Collection<BaseSamplePlan<T>> generatePlan(List<DcMeta> dcMetas) {

		Map<Pair<String, String>, BaseSamplePlan<T>> plans = new HashMap<>();

		for (DcMeta dcMeta : dcMetas) {
			for (ClusterMeta clusterMeta : dcMeta.getClusters().values()) {
				for (ShardMeta shardMeta : clusterMeta.getShards().values()) {
					Pair<String, String> cs = new Pair<>(clusterMeta.getId(), shardMeta.getId());
					BaseSamplePlan<T> plan = plans.get(cs);
					if (plan == null) {
						plan = createPlan(clusterMeta.getId(), shardMeta.getId());
						plans.put(cs, plan);
					}

					for (RedisMeta redisMeta : shardMeta.getRedises()) {

						log.debug("[generatePlan]{}", redisMeta.desc());
						addRedis(plan, dcMeta.getId(), redisMeta);
					}
				}
			}
		}
		return plans.values();
	}

	protected abstract void addRedis(BaseSamplePlan<T> plan, String dcId, RedisMeta redisMeta);

	protected abstract BaseSamplePlan<T> createPlan(String clusterId, String shardId);

}
