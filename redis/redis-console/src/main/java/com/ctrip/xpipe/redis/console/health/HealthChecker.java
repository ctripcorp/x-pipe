package com.ctrip.xpipe.redis.console.health;

import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.google.common.collect.Lists;
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
import java.util.Set;

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

	private Thread daemonHealthCheckThread;

	@PostConstruct
	public void start() {
		log.info("Redis health checker started");

		daemonHealthCheckThread = XpipeThreadFactory.create("RedisHealthChecker", true).newThread(new Runnable() {

			@Override
			public void run() {

				while (!Thread.currentThread().isInterrupted()) {
					try {
						Thread.sleep(config.getRedisReplicationHealthCheckInterval());
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
						break;
					}

					try {
						List<DcMeta> dcsToCheck = dcsToCheck(metaCache.getXpipeMeta());
						if(!dcsToCheck.isEmpty()){
							sampleAll(dcsToCheck);
						}
					} catch (Throwable e) {
						log.error("Unexpected error when sample all", e);
					}
				}
			}

		});
		daemonHealthCheckThread.start();
	}

	@VisibleForTesting
	protected List<DcMeta> dcsToCheck(XpipeMeta xpipeMeta) {
		List<DcMeta> result = new LinkedList<>(xpipeMeta.getDcs().values());
		Set<String> ignoredDcNames = config.getIgnoredHealthCheckDc();
		List<DcMeta> toRemove = Lists.newArrayList();
		for(DcMeta dcMeta : result) {
			if(ignoredDcNames.contains(dcMeta.getId()) || ignoredDcNames.contains(dcMeta.getId().toUpperCase())) {
				toRemove.add(dcMeta);
			}
		}
		result.removeAll(toRemove);
		return result;
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
