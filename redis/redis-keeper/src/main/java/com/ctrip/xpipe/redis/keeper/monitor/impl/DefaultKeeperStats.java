package com.ctrip.xpipe.redis.keeper.monitor.impl;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.lifecycle.AbstractStartStoppable;
import com.ctrip.xpipe.redis.core.monitor.BaseInstantaneousMetric;
import com.ctrip.xpipe.redis.core.monitor.InstantaneousMetric;
import com.ctrip.xpipe.redis.keeper.monitor.KeeperStats;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author wenchao.meng
 *
 * Feb 20, 2017
 */
public class DefaultKeeperStats extends AbstractStartStoppable implements KeeperStats {
	
	private AtomicLong fullSyncCount = new AtomicLong();
	
	private AtomicLong partialSyncCount = new AtomicLong();

	private AtomicLong partialSyncErrorCount = new AtomicLong();

	private AtomicLong waitOffsetSucceed = new AtomicLong();

	private AtomicLong waitOffsetFail = new AtomicLong();

	private AtomicLong inputBytes = new AtomicLong();

	private AtomicLong outputBytes = new AtomicLong();

	private ScheduledExecutorService scheduled;

	private ScheduledFuture future;

	private InstantaneousMetric inputBytesInstantaneousMetric = new BaseInstantaneousMetric();

	private InstantaneousMetric outputBytesInstantaneousMetric = new BaseInstantaneousMetric();

	public DefaultKeeperStats(ScheduledExecutorService scheduled) {
		this.scheduled = scheduled;
	}

	@Override
	public void increaseFullSync() {
		fullSyncCount.incrementAndGet();
	}

	@Override
	public long getFullSyncCount() {
		return fullSyncCount.get();
	}

	@Override
	public void increatePartialSync() {
		partialSyncCount.incrementAndGet();
	}

	@Override
	public long getPartialSyncCount() {
		return partialSyncCount.get();
	}

	@Override
	public void increatePartialSyncError() {
		partialSyncErrorCount.incrementAndGet();
	}

	@Override
	public long getPartialSyncErrorCount() {
		return partialSyncErrorCount.get();
	}

	@Override
	public long increaseWaitOffsetSucceed() {
		return waitOffsetSucceed.incrementAndGet();
	}

	@Override
	public long increasWaitOffsetFail() {
		return waitOffsetFail.incrementAndGet();
	}

	@Override
	public long getWaitOffsetSucceed() {
		return waitOffsetSucceed.get();
	}

	@Override
	public long getWaitOffsetFail() {
		return waitOffsetFail.get();
	}

	@Override
	public long getInputInstantaneousBPS() {
		return inputBytesInstantaneousMetric.getInstantaneousMetric();
	}

	@Override
	public long getOutputInstantaneousBPS() {
		return outputBytesInstantaneousMetric.getInstantaneousMetric();
	}

	@Override
	public void increaseInputBytes(long bytes) {
		inputBytes.addAndGet(bytes);
	}

	@Override
	public void increaseOutputBytes(long bytes) {
		outputBytes.addAndGet(bytes);
	}

	@Override
	public long getInputBytes() {
		return inputBytes.get();
	}

	@Override
	public long getOutputBytes() {
		return outputBytes.get();
	}

	private void updatePerSec() {
		int interval = 100;
		future = scheduled.scheduleAtFixedRate(new AbstractExceptionLogTask() {
			@Override
			protected void doRun() {
				inputBytesInstantaneousMetric.trackInstantaneousMetric(inputBytes.get());
				outputBytesInstantaneousMetric.trackInstantaneousMetric(outputBytes.get());
			}
		}, interval, interval, TimeUnit.MILLISECONDS);
	}

	@Override
	protected void doStart() throws Exception {
		updatePerSec();
	}

	@Override
	protected void doStop() throws Exception {
		if(future != null) {
			future.cancel(true);
		}
	}
}
