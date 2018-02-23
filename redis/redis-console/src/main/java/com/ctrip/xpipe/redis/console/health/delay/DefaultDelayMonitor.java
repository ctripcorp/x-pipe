package com.ctrip.xpipe.redis.console.health.delay;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.health.*;
import com.ctrip.xpipe.redis.console.health.ping.PingService;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map.Entry;

/**
 * @author marsqing
 *
 *         Dec 1, 2016 3:02:05 PM
 */
@Component
@Lazy
public class DefaultDelayMonitor extends BaseSampleMonitor<InstanceDelayResult> implements DelayMonitor {

	public static final String CHECK_CHANNEL = "xpipe-health-check";

	public static final long SAMPLE_LOST_AND_NO_PONG = -99999L * 1000 * 1000;

	public static final long SAMPLE_LOST_BUT_PONG = 99999L * 1000 * 1000;

	@Autowired
	private List<DelayCollector> delayCollectors;

	@Autowired
	private PingService pingSvc;


	@Override
	public void startSample(BaseSamplePlan<InstanceDelayResult> plan) throws SampleException {
		sampleDelay((DelaySamplePlan) plan);
	}

	@Override
	protected void notifyCollectors(Sample<InstanceDelayResult> sample) {
		DelaySampleResult sampleResult = convertToSampleResult(sample);
		for (DelayCollector collector : delayCollectors) {
			try {
				collector.collect(sampleResult);
			} catch (Exception e) {
				log.error("[DefaultDelayMonitor][notifyCollectors]{}", e);
			}
		}
	}

	private DelaySampleResult convertToSampleResult(Sample<InstanceDelayResult> sample) {
		BaseSamplePlan<InstanceDelayResult> plan = sample.getSamplePlan();
		DelaySampleResult sampleResult = new DelaySampleResult(sample.getStartTime(), plan.getClusterId(), plan.getShardId());

		for (Entry<HostPort, InstanceDelayResult> entry : plan.getHostPort2SampleResult().entrySet()) {
			HostPort hostPort = entry.getKey();
			InstanceDelayResult delay = entry.getValue();

			long delayNanos = SAMPLE_LOST_AND_NO_PONG;
			if (delay.isDone()) {
				delayNanos = delay.calculateDelay(sample.getStartNanoTime());
			} else {
				if (pingSvc.isRedisAlive(hostPort)) {
					delayNanos = SAMPLE_LOST_BUT_PONG;
				}
			}

			if (delay.isMaster()) {
				sampleResult.setMasterDelayNanos(hostPort, delayNanos);
			} else {
				sampleResult.addSlaveDelayNanos(hostPort, delayNanos);
			}
		}

		return sampleResult;
	}

	private void sampleDelay(final DelaySamplePlan samplePlan) {

		if (samplePlan.getHostPort2SampleResult().isEmpty()) {
			return;
		}

		for (final HostPort hostPort : samplePlan.getHostPort2SampleResult().keySet()) {
				RedisSession session = findRedisSession(hostPort.getHost(), hostPort.getPort());
				session.subscribeIfAbsent(CHECK_CHANNEL, new RedisSession.SubscribeCallback() {

					@Override
					public void message(String channel, String message) {
						log.debug("[sampleDelay][message]{}, {}", hostPort, message);
						addInstanceSuccess(Long.parseLong(message, 16), hostPort.getHost(), hostPort.getPort(), null);
					}

					@Override
					public void fail(Throwable e) {
						//nothing to do
					}
				});
		}

		RedisSession masterSession = null;
		if(samplePlan.getMasterHost() != null) {
			masterSession = findRedisSession(samplePlan.getMasterHost(), samplePlan.getMasterPort());
		}
		long startNanoTime = recordSample(samplePlan);
		log.debug("[sampleDelay][publish]{}:{}", samplePlan.getMasterHost(), samplePlan.getMasterPort());
		if(masterSession != null) {
			masterSession.publish(CHECK_CHANNEL, Long.toHexString(startNanoTime));
		}
	}

	@Override
	protected void addRedis(BaseSamplePlan<InstanceDelayResult> plan, String dcId, RedisMeta redisMeta) {

		DelaySamplePlan delaySamplePlan = (DelaySamplePlan) plan;
		delaySamplePlan.addRedis(dcId, redisMeta);
	}

	@Override
	protected BaseSamplePlan<InstanceDelayResult> createPlan(String dcId, String clusterId, String shardId) {
		return new DelaySamplePlan(clusterId, shardId);
	}

	@VisibleForTesting
	protected void setDelayCollectors(List<DelayCollector> collectors) {
		this.delayCollectors = collectors;
	}

	@VisibleForTesting
	protected List<DelayCollector> getDelayCollectors() {
		return this.delayCollectors;
	}
}
