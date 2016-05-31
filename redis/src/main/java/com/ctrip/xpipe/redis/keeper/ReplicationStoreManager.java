package com.ctrip.xpipe.redis.keeper;

/**
 * @author wenchao.meng
 *
 * May 31, 2016
 */
public interface ReplicationStoreManager {
	
	/**
	 * create new replication store
	 * @return
	 */
	ReplicationStore create();
	
	/**
	 * get the newest replication store
	 * @return
	 */
	ReplicationStore getCurrent();
	
	
	void destroy(ReplicationStore replicationStore);
	

	String getClusterName();
	
	String getShardName();
}
