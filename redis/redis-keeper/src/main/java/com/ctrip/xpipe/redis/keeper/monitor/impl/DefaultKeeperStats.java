package com.ctrip.xpipe.redis.keeper.monitor.impl;

import com.ctrip.xpipe.redis.keeper.monitor.KeeperStats;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author wenchao.meng
 *
 * Feb 20, 2017
 */
public class DefaultKeeperStats implements KeeperStats{
	
	private AtomicLong fullSyncCount = new AtomicLong();
	
	private AtomicLong partialSyncCount = new AtomicLong();

	private AtomicLong partialSyncErrorCount = new AtomicLong();

	private AtomicLong waitOffsetSucceed = new AtomicLong();

	private AtomicLong waitOffsetFail = new AtomicLong();

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
}
