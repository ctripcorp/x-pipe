package com.ctrip.xpipe.redis.console.health.ping;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.health.*;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map.Entry;

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
	public void startSample(BaseSamplePlan<InstancePingResult> plan) throws SampleException {

		long startNanoTime = recordSample(plan);
		samplePing(startNanoTime, plan);
	}

	private void samplePing(final long startNanoTime, BaseSamplePlan<InstancePingResult> plan) {

		for (Entry<HostPort, InstancePingResult> entry : plan.getHostPort2SampleResult().entrySet()) {

			final HostPort hostPort = entry.getKey();
			log.debug("[ping]{}", hostPort);

			try{
				RedisSession session = findRedisSession(hostPort.getHost(), hostPort.getPort());
				session.ping(new PingCallback() {

					@Override
					public void pong(String pongMsg) {
						addInstanceSuccess(startNanoTime, hostPort.getHost(), hostPort.getPort(), null);
					}

					@Override
					public void fail(Throwable th) {
						addInstanceFail(startNanoTime, hostPort.getHost(), hostPort.getPort(), th);
					}
				});
			}catch (Exception e){
				log.error("[samplePing]" + hostPort, e);
			}
		}
	}

	@Override
	protected void addRedis(BaseSamplePlan<InstancePingResult> plan, String dcId, RedisMeta redisMeta) {

		plan.addRedis(dcId, redisMeta, new InstancePingResult());
	}

	@Override
	protected BaseSamplePlan<InstancePingResult> createPlan(String dcId, String clusterId, String shardId) {

		return new PingSamplePlan(clusterId, shardId);
	}

}
