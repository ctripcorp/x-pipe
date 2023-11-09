package com.ctrip.xpipe.redis.keeper.monitor.impl;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.lifecycle.AbstractStartStoppable;
import com.ctrip.xpipe.redis.core.monitor.BaseInstantaneousMetric;
import com.ctrip.xpipe.redis.core.monitor.InstantaneousMetric;
import com.ctrip.xpipe.redis.keeper.monitor.KeeperStats;
import com.ctrip.xpipe.redis.keeper.monitor.PsyncFailReason;

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

	private String replId;
	
	private AtomicLong fullSyncCount = new AtomicLong();
	
	private AtomicLong partialSyncCount = new AtomicLong();

	private AtomicLong partialSyncErrorCount = new AtomicLong();

	private AtomicLong waitOffsetSucceed = new AtomicLong();

	private AtomicLong waitOffsetFail = new AtomicLong();

	private AtomicLong inputBytes = new AtomicLong();

	private AtomicLong outputBytes = new AtomicLong();

	private ScheduledExecutorService scheduled;

	private ScheduledFuture<?> future;

	private InstantaneousMetric inputBytesInstantaneousMetric = new BaseInstantaneousMetric();

	private InstantaneousMetric outputBytesInstantaneousMetric = new BaseInstantaneousMetric();

	private PsyncFailReason lastFailReason;

	private AtomicLong psyncSendFailCount = new AtomicLong();

	private AtomicLong peakInputInstantaneousInput = new AtomicLong();

	private AtomicLong peakOutputInstantaneousOutput = new AtomicLong();

	public DefaultKeeperStats(String replId, ScheduledExecutorService scheduled) {
		this.replId = replId;
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

	@Override
	public long getPeakInputInstantaneousBPS() {
		return peakInputInstantaneousInput.get();
	}

	@Override
	public long getPeakOutputInstantaneousBPS() {
		return peakOutputInstantaneousOutput.get();
	}

	@Override
	public void increasePsyncSendFail() {
		psyncSendFailCount.incrementAndGet();
	}

	@Override
	public long getPsyncSendFailCount() {
		return psyncSendFailCount.get();
	}

	@Override
	public void setLastPsyncFailReason(PsyncFailReason reason) {
		this.lastFailReason = reason;
	}

	@Override
	public PsyncFailReason getLastPsyncFailReason() {
		return this.lastFailReason;
	}

	private void updateTrafficStats() {
		int interval = 100;
		future = scheduled.scheduleAtFixedRate(new AbstractExceptionLogTask() {
			@Override
			protected void doRun() {
				updateInstantaneousMetric();
				updatePeakStats();
				logStats();
			}
		}, interval, interval, TimeUnit.MILLISECONDS);
	}

	private void updateInstantaneousMetric() {
		inputBytesInstantaneousMetric.trackInstantaneousMetric(inputBytes.get());
		outputBytesInstantaneousMetric.trackInstantaneousMetric(outputBytes.get());
	}

	private void updatePeakStats() {
		long inputBPS = getInputInstantaneousBPS();
		if (inputBPS > getPeakInputInstantaneousBPS()) {
			peakInputInstantaneousInput.set(inputBPS);
		}
		long outputBPS = getOutputInstantaneousBPS();
		if (outputBPS > getPeakOutputInstantaneousBPS()) {
			peakOutputInstantaneousOutput.set(outputBPS);
		}
	}

	private void logStats() {
		logger.debug("[{}][input]{}", replId, getInputInstantaneousBPS());
		logger.debug("[{}][output]{}", replId, getOutputInstantaneousBPS());
		logger.debug("[{}][peak-in]{}", replId, getPeakInputInstantaneousBPS());
		logger.debug("[{}][peak-out]{}", replId, getPeakOutputInstantaneousBPS());
	}

	@Override
	protected void doStart() throws Exception {
		updateTrafficStats();
	}

	@Override
	protected void doStop() throws Exception {
		if(future != null) {
			future.cancel(true);
		}
	}
}
