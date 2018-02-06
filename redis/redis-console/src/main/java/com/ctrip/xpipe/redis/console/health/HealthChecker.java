package com.ctrip.xpipe.redis.console.health;

import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author marsqing
 *
 *         Nov 30, 2016 10:11:51 PM
 */
@Component
@ConditionalOnProperty(name = { HealthChecker.ENABLED }, matchIfMissing = true)
public class HealthChecker {

	public final static String ENABLED = "redis.health.check.enabled";

	private static Logger log = LoggerFactory.getLogger(HealthChecker.class);

	@Autowired
	private MetaCache metaCache;

	@Autowired
	private List<SampleMonitor> sampleMonitors;

	@Autowired
	private ConsoleConfig config;

	@Autowired
	private HealthCheckVisitor healthCheckVisitor;

	@Autowired
	private DefaultRedisSessionManager sessionManager;

	private Thread daemonHealthCheckThread;

	@PostConstruct
	public void start() {
		log.info("Redis health checker started");

		daemonHealthCheckThread = XpipeThreadFactory.create("RedisHealthChecker", true).newThread(new Runnable() {

			private boolean warmuped = false;

			@Override
			public void run() {

				while (!Thread.currentThread().isInterrupted()) {
					try {
						if(!warmuped) {
							TimeUnit.SECONDS.sleep(2);
							warmup();
							warmuped = true;
							waitAndRetry();
						}
						List<DcMeta> dcsToCheck = new LinkedList<>(metaCache.getXpipeMeta().getDcs().values());
						if(!dcsToCheck.isEmpty()){
							sampleAll(dcsToCheck);
						}
					} catch (Throwable e) {
						log.error("Unexpected error when sample all", e);
					}

					try {
						Thread.sleep(config.getRedisReplicationHealthCheckInterval());
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
						break;
					}
				}
			}

			public void waitAndRetry() {
				int retryTimes = 4;
				while(retryTimes > 0) {
					retryTimes --;
					try {
						TimeUnit.SECONDS.sleep(6);
					} catch (InterruptedException ignore) {
					}
					ThreadPoolExecutor executor = (ThreadPoolExecutor)sessionManager.getExecutors();
					log.info("[warmup] redis connection thread pool: {}", executor.toString());
					if(executor.getPoolSize() != 0 && executor.getQueue().size() == 0) {
						break;
					}
				}
			}

		});
		daemonHealthCheckThread.start();
	}

	@VisibleForTesting
	protected void warmup() {
		int period = 1000;
		XpipeMeta xpipeMeta = null;
		while(xpipeMeta == null) {
			log.info("[warmup] waiting for metaCache initialized");
			try {
				Thread.sleep(period);
				xpipeMeta = metaCache.getXpipeMeta();
				Thread.sleep(period);
			} catch (Exception e) {
				log.error("[warmup]", e);
			}
		}
		try {
			List<DcMeta> dcsToCheck = new LinkedList<>(xpipeMeta.getDcs().values());
			for(DcMeta dc : dcsToCheck) {
				dc.accept(healthCheckVisitor);
			}
		} catch (Exception e) {
			log.error("[warmup] error: {}", e);
		}
	}

	private void sampleAll(List<DcMeta> dcMetas) {

		for(SampleMonitor sampleMonitor : sampleMonitors){

			log.debug("[sampleAll]{}", sampleMonitor);
			Collection collection = sampleMonitor.generatePlan(dcMetas);
			if(collection == null){
				continue;
			}

			for(Object objectPlan : collection){

				BaseSamplePlan plan = (BaseSamplePlan) objectPlan;
				try {
					sampleMonitor.startSample(plan);
				} catch (Exception e) {
					log.error(String.format("Error sample %s of cluster:%s shard:%s", sampleMonitor, plan.getClusterId(), plan.getShardId()), e);
				}
			}
		}
	}

	@PreDestroy
	public void preDestroy() {
		daemonHealthCheckThread.interrupt();
	}

	@VisibleForTesting
	public void setMetaCache(MetaCache metaCache) {
		this.metaCache = metaCache;
	}
}
