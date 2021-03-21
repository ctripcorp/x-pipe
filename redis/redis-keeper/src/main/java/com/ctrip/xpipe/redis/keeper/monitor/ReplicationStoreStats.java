package com.ctrip.xpipe.redis.keeper.monitor;

import com.ctrip.xpipe.redis.core.protocal.MASTER_STATE;

/**
 * @author wenchao.meng
 *
 * Mar 10, 2017
 */
public interface ReplicationStoreStats {


	void increateReplicationStoreCreateCount();
	
	long getReplicationStoreCreateCount();

	void setMasterState(MASTER_STATE masterState);
	long getLastReplDownTime();
}
