package com.ctrip.xpipe.redis.core.store;

import java.io.IOException;

import com.ctrip.xpipe.api.lifecycle.Destroyable;

/**
 * @author wenchao.meng
 *
 * May 31, 2016
 */
public interface ReplicationStoreManager  extends Destroyable{
	
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

	ReplicationStore create(String masterRunid, long keeperBeginOffset) throws IOException;
}
