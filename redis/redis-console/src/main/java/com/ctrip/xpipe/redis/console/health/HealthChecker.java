package com.ctrip.xpipe.redis.console.health;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import org.unidal.tuple.Pair;

import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.health.delay.DelayMonitor;
import com.ctrip.xpipe.redis.console.health.delay.DelaySamplePlan;
import com.ctrip.xpipe.redis.console.health.ping.InstancePingResult;
import com.ctrip.xpipe.redis.console.health.ping.PingMonitor;
import com.ctrip.xpipe.redis.console.health.ping.PingSamplePlan;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.meta.DcMetaService;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.utils.XpipeThreadFactory;

/**
 * @author marsqing
 *
 *         Nov 30, 2016 10:11:51 PM
 */
@Component
@ConditionalOnProperty(name = { HealthChecker.ENABLED })
public class HealthChecker {

	public final static String ENABLED = "redis.health.check.enabled";

	private static Logger log = LoggerFactory.getLogger(HealthChecker.class);

	@Autowired
	private DcMetaService dcMetaService;

	@Autowired
	private DcService dcService;

	@Autowired
	private DelayMonitor delayMonitor;

	@Autowired
	private PingMonitor pingMonitor;

	@Autowired
	private ConsoleConfig config;

	@PostConstruct
	public void start() {
		log.info("Redis health checker started");

		XpipeThreadFactory.create("RedisHealthChecker", true).newThread(new Runnable() {

			@Override
			public void run() {
				long dcListLastUpdateTime = 0;
				List<DcMeta> dcsToCheck = Collections.emptyList();

				while (!Thread.currentThread().isInterrupted()) {
					// TODO cache DcMeta in DcMetaService to avoid throttle
					if (System.currentTimeMillis() - dcListLastUpdateTime > 30000) {
						try {
							dcsToCheck = findDcsToCheck();
						} catch (Exception e) {
							log.error("Error update dc list to health check, will use last dc list", e);
						}
					}

					try {
						sampleAll(dcsToCheck);
					} catch (Exception e) {
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
		for (DelaySamplePlan plan : generateDelaySamplePlans(dcMetas)) {
			try {
				delayMonitor.startSample(plan);
			} catch (Exception e) {
				log.error("Error sample delay of cluster:{} shard:{}", plan.getClusterId(), plan.getShardId(), e);
			}
		}

		for (PingSamplePlan plan : generatePingSamplePlans(dcMetas)) {
			try {
				pingMonitor.startSample(plan);
			} catch (Exception e) {
				log.error("Error sample ping of cluster:{} shard:{}", plan.getClusterId(), plan.getShardId(), e);
			}
		}
	}

	private Collection<PingSamplePlan> generatePingSamplePlans(List<DcMeta> dcMetas) {
		Map<Pair<String, String>, PingSamplePlan> plans = new HashMap<>();

		for (DcMeta dcMeta : dcMetas) {
			for (ClusterMeta clusterMeta : dcMeta.getClusters().values()) {
				for (ShardMeta shardMeta : clusterMeta.getShards().values()) {
					Pair<String, String> cs = new Pair<>(clusterMeta.getId(), shardMeta.getId());
					PingSamplePlan plan = plans.get(cs);
					if (plan == null) {
						plan = new PingSamplePlan(clusterMeta.getId(), shardMeta.getId());
						plans.put(cs, plan);
					}

					for (RedisMeta redisMeta : shardMeta.getRedises()) {
						plan.addRedis(dcMeta.getId(), redisMeta, new InstancePingResult());
					}
				}
			}
		}

		return plans.values();
	}

	private Collection<DelaySamplePlan> generateDelaySamplePlans(List<DcMeta> dcMetas) {
		Map<Pair<String, String>, DelaySamplePlan> plans = new HashMap<>();

		for (DcMeta dcMeta : dcMetas) {
			for (ClusterMeta clusterMeta : dcMeta.getClusters().values()) {
				for (ShardMeta shardMeta : clusterMeta.getShards().values()) {
					Pair<String, String> cs = new Pair<>(clusterMeta.getId(), shardMeta.getId());
					DelaySamplePlan plan = plans.get(cs);
					if (plan == null) {
						plan = new DelaySamplePlan(clusterMeta.getId(), shardMeta.getId());
						plans.put(cs, plan);
					}

					for (RedisMeta redisMeta : shardMeta.getRedises()) {
						plan.addRedis(dcMeta.getId(), redisMeta);
					}
				}
			}
		}

		return plans.values();
	}

	private List<DcMeta> findDcsToCheck() {
		List<DcTbl> dcs = dcService.findAllDcNames();

		List<DcMeta> dcMetas = new LinkedList<>();
		for (DcTbl dc : dcs) {
			dcMetas.add(dcMetaService.getDcMeta(dc.getDcName()));
		}

		return dcMetas;
	}

}
