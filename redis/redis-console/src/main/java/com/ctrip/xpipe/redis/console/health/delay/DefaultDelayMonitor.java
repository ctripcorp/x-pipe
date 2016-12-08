package com.ctrip.xpipe.redis.console.health.delay;

import java.util.List;
import java.util.Map.Entry;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import com.ctrip.xpipe.metric.HostPort;
import com.ctrip.xpipe.redis.console.health.BaseSampleMonitor;
import com.ctrip.xpipe.redis.console.health.BaseSamplePlan;
import com.ctrip.xpipe.redis.console.health.RedisSession;
import com.ctrip.xpipe.redis.console.health.Sample;
import com.ctrip.xpipe.redis.console.health.ping.PingService;
import com.lambdaworks.redis.pubsub.RedisPubSubAdapter;

/**
 * @author marsqing
 *
 *         Dec 1, 2016 3:02:05 PM
 */
@Component
@Lazy
public class DefaultDelayMonitor extends BaseSampleMonitor<InstanceDelayResult> implements DelayMonitor {

	public static final String CHECK_CHANNEL = "xpipe-health-check";

	public static final long SAMPLE_LOST_AND_NO_PONG = -99L * 1000 * 1000;

	private static final long SAMPLE_LOST_BUT_PONG = 99999L * 1000 * 1000;

	@Autowired
	private List<DelayCollector> delayCollectors;

	@Autowired
	private PingService pingSvc;

	@Override
	public void startSample(BaseSamplePlan<InstanceDelayResult> plan) throws Exception {
		sampleDelay((DelaySamplePlan) plan);
	}

	@Override
	protected void notifyCollectors(Sample<InstanceDelayResult> sample) {
		DelaySampleResult sampleResult = convertToSampleResult(sample);
		for (DelayCollector collector : delayCollectors) {
			collector.collect(sampleResult);
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

	private void sampleDelay(final DelaySamplePlan samplePlan) throws Exception {
		if (samplePlan.getHostPort2SampleResult().isEmpty()) {
			return;
		}

		for (final HostPort hostPort : samplePlan.getHostPort2SampleResult().keySet()) {
			RedisSession session = findRedisSession(hostPort.getHost(), hostPort.getPort());
			session.subscribeIfAbsent(CHECK_CHANNEL, new RedisPubSubAdapter<String, String>() {

				@Override
				public void message(String channel, String message) {
					addInstanceResult(Long.parseLong(message, 16), hostPort.getHost(), hostPort.getPort(), null);
				}

			});

		}

		RedisSession masterSession = findRedisSession(samplePlan.getMasterHost(), samplePlan.getMasterPort());
		long startNanoTime = recordSample(samplePlan);
		masterSession.publish(CHECK_CHANNEL, Long.toHexString(startNanoTime));
	}

}
