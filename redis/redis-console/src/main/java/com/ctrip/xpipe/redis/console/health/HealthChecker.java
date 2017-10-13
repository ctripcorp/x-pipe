package com.ctrip.xpipe.redis.console.health;

import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

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

	@PostConstruct
	public void start() {
		log.info("Redis health checker started");

		XpipeThreadFactory.create("RedisHealthChecker", true).newThread(new Runnable() {

			@Override
			public void run() {

				while (!Thread.currentThread().isInterrupted()) {

					try {
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

		}).start();
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

}
