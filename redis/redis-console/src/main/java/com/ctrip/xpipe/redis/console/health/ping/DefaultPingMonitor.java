package com.ctrip.xpipe.redis.console.health.ping;

import java.util.List;
import java.util.Map.Entry;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import com.ctrip.xpipe.metric.HostPort;
import com.ctrip.xpipe.redis.console.health.BaseSampleMonitor;
import com.ctrip.xpipe.redis.console.health.BaseSamplePlan;
import com.ctrip.xpipe.redis.console.health.PingCallback;
import com.ctrip.xpipe.redis.console.health.RedisSession;
import com.ctrip.xpipe.redis.console.health.Sample;

/**
 * @author marsqing
 *
 *         Dec 2, 2016 5:57:36 PM
 */
@Component
@Lazy
public class DefaultPingMonitor extends BaseSampleMonitor<InstancePingResult> implements PingMonitor {

	@Autowired
	private List<PingCollector> collectors;

	@Override
	protected void notifyCollectors(Sample<InstancePingResult> sample) {
		PingSampleResult sampleResult = converToSampleResult(sample);
		for (PingCollector collector : collectors) {
			collector.collect(sampleResult);
		}
	}

	private PingSampleResult converToSampleResult(Sample<InstancePingResult> sample) {
		BaseSamplePlan<InstancePingResult> plan = sample.getSamplePlan();
		PingSampleResult result = new PingSampleResult(plan.getClusterId(), plan.getShardId());

		for (Entry<HostPort, InstancePingResult> entry : sample.getSamplePlan().getHostPort2SampleResult().entrySet()) {
			HostPort hostPort = entry.getKey();
			result.addPong(hostPort, entry.getValue());
		}

		return result;
	}

	@Override
	public void startSample(BaseSamplePlan<InstancePingResult> plan) throws Exception {
		long startNanoTime = recordSample(plan);
		samplePing(startNanoTime, plan);
	}

	private void samplePing(final long startNanoTime, BaseSamplePlan<InstancePingResult> plan) {
		for (Entry<HostPort, InstancePingResult> entry : plan.getHostPort2SampleResult().entrySet()) {
			final HostPort hostPort = entry.getKey();
			RedisSession session = findRedisSession(hostPort.getHost(), hostPort.getPort());

			session.ping(new PingCallback() {

				@Override
				public void pong(boolean pong, String pongMsg) {
					addInstanceResult(startNanoTime, hostPort.getHost(), hostPort.getPort(), null);
				}
			});
		}
	}

}
