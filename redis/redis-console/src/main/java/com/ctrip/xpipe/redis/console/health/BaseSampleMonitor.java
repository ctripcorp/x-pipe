package com.ctrip.xpipe.redis.console.health;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.concurrent.DefaultExecutorFactory;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

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
	protected RedisSessionManager redisSessionManager;

	@Autowired
	protected SampleClassfier sampleClassfier;

	@Autowired
	protected HealthCheckEndpointManager healthCheckEndpointManager;

	protected Map<SampleKey, Sample> samples = new ConcurrentHashMap<>();

	protected Thread daemonThread;

	private ExecutorService executors;

	@PostConstruct
	public void postBaseSampleMonitor(){
		executors = DefaultExecutorFactory.createAllowCoreTimeoutAbortPolicy("Collector-" + getClass().getSimpleName()).createExecutorService();
	}

	@PreDestroy
	public void preBaseSampleMonitor(){
		executors.shutdown();
	}


	protected abstract void notifyCollectors(Sample<T> sample);

	protected long recordSample(BaseSamplePlan<T> plan) {
		long nanoTime = System.nanoTime();
		samples.putAll(sampleClassfier.getClassifiedSamples(nanoTime, plan));
		return nanoTime;
	}

	protected RedisSession findRedisSession(HealthCheckEndpoint endpoint) {
		return redisSessionManager.findOrCreateSession(endpoint);
	}

	protected <C> void addInstanceSuccess(long nanoTime, HealthCheckEndpoint endpoint, C context) {
		Sample sample = samples.get(new SampleKey(nanoTime, endpoint.getDelayCheckTimeoutMilli()));
		if (sample != null) {
			sample.addInstanceSuccess(endpoint, context);
		}
	}

	protected <C> void addInstanceFail(long nanoTime, HealthCheckEndpoint endpoint, Throwable th) {
		Sample sample = samples.get(new SampleKey(nanoTime, endpoint.getDelayCheckTimeoutMilli()));
		if (sample != null) {
			sample.addInstanceFail(endpoint, th);
		}
	}

	@PostConstruct
	public void scanSamples() {
		daemonThread = XpipeThreadFactory.create("SampleMonitor-" + getClass().getSimpleName(), true).newThread(new Runnable() {

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


				long start = System.currentTimeMillis();
				int  count = 0;

				Iterator<Entry<SampleKey, Sample>> iter = samples.entrySet().iterator();
				while (iter.hasNext()) {
					count++;
					Sample sample = iter.next().getValue();

					if (sample.isDone() || sample.isExpired()) {
						try {
							executors.execute(new AbstractExceptionLogTask() {
								@Override
								protected void doRun() throws Exception {
									notifyCollectors(sample);
								}
							});
						} catch (Exception e) {
							log.error("Exception caught from notified collectors", e);
						}
						iter.remove();
					}
				}
				long end = System.currentTimeMillis();
				long cost = end - start;
				if(cost > 10){
					log.info("[scan end][cost > 10 ms]count:{}, cost: {} ms", count, end - start);
				}

			}

		});
		daemonThread.start();
	}

	@Override
	public Collection<BaseSamplePlan<T>> generatePlan(List<DcMeta> dcMetas) {

		Map<Pair<String, String>, BaseSamplePlan<T>> plans = new HashMap<>();

		for (DcMeta dcMeta : dcMetas) {
			for (ClusterMeta clusterMeta : dcMeta.getClusters().values()) {
				if(!addCluster(dcMeta.getId(), clusterMeta)){
					continue;
				}
				for (ShardMeta shardMeta : clusterMeta.getShards().values()) {
					Pair<String, String> cs = new Pair<>(clusterMeta.getId(), shardMeta.getId());
					BaseSamplePlan<T> plan = plans.get(cs);
					if (plan == null) {
						plan = createPlan(dcMeta.getId(), clusterMeta.getId(), shardMeta.getId());
						plans.put(cs, plan);
					}

					for (RedisMeta redisMeta : shardMeta.getRedises()) {

						log.debug("[generatePlan]{}", redisMeta.desc());
						addRedis(plan, dcMeta.getId(), healthCheckEndpointManager.getOrCreate(redisMeta));
					}

					if(plan.isEmpty()) {
						plans.remove(cs);
					}
				}
			}
		}
		return plans.values();
	}

	protected boolean addCluster(String dcName, ClusterMeta clusterMeta) {
		return true;
	}

	protected abstract void addRedis(BaseSamplePlan<T> plan, String dcId, HealthCheckEndpoint endpoint);

	protected abstract BaseSamplePlan<T> createPlan(String dcId, String clusterId, String shardId);

	@VisibleForTesting
	public void setSamples(Map<SampleKey, Sample> samples) {
		this.samples = samples;
	}

	@PreDestroy
	public void preDestroy() {
		daemonThread.interrupt();
	}
}
