package com.ctrip.xpipe.redis.console.health;

import com.ctrip.xpipe.endpoint.HostPort;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author marsqing
 *
 *         Dec 6, 2016 1:59:22 PM
 */
@SuppressWarnings("rawtypes")
public class Sample<T extends BaseInstanceResult> {

	private long startTime;
	private long expireTime;
	protected BaseSamplePlan<T> samplePlan;
	private long startNanoTime;
	private AtomicInteger remainingRedisCount;

	public Sample(long startTime, long startNanoTime, BaseSamplePlan<T> samplePlan, int expireDelayMillis, int totalInstanceCount) {
		this.startTime = startTime;
		this.samplePlan = samplePlan;
		expireTime = startTime + expireDelayMillis;
		this.startNanoTime = startNanoTime;
		remainingRedisCount = new AtomicInteger(totalInstanceCount);
	}

	@SuppressWarnings("unchecked")
	public <C> void addInstanceSuccess(HealthCheckEndpoint endpoint, C context) {
		BaseInstanceResult<C> instanceResult = samplePlan.findInstanceResult(endpoint);

		if (instanceResult != null && !instanceResult.isDone()) {
			instanceResult.success(System.nanoTime(), context);
			remainingRedisCount.decrementAndGet();
		}
	}

	public <C> void addInstanceFail(HealthCheckEndpoint endpoint, Throwable th) {
		BaseInstanceResult<C> instanceResult = samplePlan.findInstanceResult(endpoint);

		if (instanceResult != null && !instanceResult.isDone()) {
			instanceResult.fail(System.nanoTime(), th);
			remainingRedisCount.decrementAndGet();
		}
	}

	public long getStartNanoTime() {
		return startNanoTime;
	}

	public long getStartTime() {
		return startTime;
	}

	public BaseSamplePlan<T> getSamplePlan() {
		return samplePlan;
	}

	public boolean isExpired() {
		return System.currentTimeMillis() > expireTime;
	}

	public boolean isDone() {
		return remainingRedisCount.get() == 0;
	}

	protected void increaseRemainingRedisCount() {
		remainingRedisCount.incrementAndGet();
	}

	protected int getRemainingRedisCount() {
		return remainingRedisCount.get();
	}
}
