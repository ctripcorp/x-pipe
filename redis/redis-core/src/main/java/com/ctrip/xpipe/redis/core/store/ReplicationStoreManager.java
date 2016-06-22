package com.ctrip.xpipe.redis.core.store;

import java.io.IOException;

/**
 * @author wenchao.meng
 *
 * May 31, 2016
 */
public interface ReplicationStoreManager {
	
	ReplicationStore createIfDirtyOrNotExist() throws IOException;
	
	ReplicationStore createIfNotExist() throws IOException;

	/**
	 * create new replication store
	 * @return
	 * @throws IOException 
	 */
	ReplicationStore create() throws IOException;
	
	/**
	 * get the newest replication store
	 * @return
	 * @throws IOException 
	 */
	ReplicationStore getCurrent() throws IOException;
	
	
	void destroy(ReplicationStore replicationStore);
	

	String getClusterName();
	
	String getShardName();
}
