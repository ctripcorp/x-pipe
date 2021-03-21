package com.ctrip.xpipe.redis.keeper.monitor.impl;

import com.ctrip.xpipe.redis.core.protocal.MASTER_STATE;
import com.ctrip.xpipe.redis.keeper.monitor.ReplicationStoreStats;
import com.ctrip.xpipe.utils.OsUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author wenchao.meng
 *
 * Mar 10, 2017
 */
public class DefaultReplicationStoreStats implements ReplicationStoreStats{

	private AtomicLong replicationStoreCreateCount = new AtomicLong();

	private MASTER_STATE masterState;
	private long 		lastReplDownTime = System.currentTimeMillis() - OsUtils.APPROXIMATE__RESTART_TIME_MILLI;//60s may be restart time

	@Override
	public void increateReplicationStoreCreateCount() {
		replicationStoreCreateCount.incrementAndGet();
		
	}

	@Override
	public long getReplicationStoreCreateCount() {
		return replicationStoreCreateCount.get();
	}

	@Override
	public void setMasterState(MASTER_STATE masterState) {

		if(this.masterState == MASTER_STATE.REDIS_REPL_CONNECTED && masterState != this.masterState){
			this.lastReplDownTime = System.currentTimeMillis();
		}

		this.masterState = masterState;
	}

	@Override
	public long getLastReplDownTime() {
		return lastReplDownTime;
	}

	@VisibleForTesting
	public void setLastReplDownTime(long lastReplDownTime) {
		this.lastReplDownTime = lastReplDownTime;
	}
}
