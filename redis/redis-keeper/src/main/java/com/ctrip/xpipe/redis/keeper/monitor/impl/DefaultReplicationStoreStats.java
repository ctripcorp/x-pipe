package com.ctrip.xpipe.redis.keeper.monitor.impl;

import com.ctrip.xpipe.redis.keeper.monitor.ReplicationStoreStats;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author wenchao.meng
 *
 * Mar 10, 2017
 */
public class DefaultReplicationStoreStats implements ReplicationStoreStats{
	
	private AtomicLong replicationStoreCreateCount = new AtomicLong();

	private AtomicLong repl_down_since = new AtomicLong(0);

	@Override
	public void increateReplicationStoreCreateCount() {
		replicationStoreCreateCount.incrementAndGet();
		
	}

	@Override
	public long getReplicationStoreCreateCount() {
		return replicationStoreCreateCount.get();
	}

	@Override
	public long getReplDownSince() {
		return repl_down_since.get();
	}

	@Override
	public void refreshReplDownSince(long replDownSince) {
		repl_down_since.set(replDownSince);
	}

}
