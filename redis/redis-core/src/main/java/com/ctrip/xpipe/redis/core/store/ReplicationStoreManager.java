package com.ctrip.xpipe.redis.core.store;

import com.ctrip.xpipe.api.lifecycle.Destroyable;
import com.ctrip.xpipe.api.lifecycle.Lifecycle;
import com.ctrip.xpipe.api.observer.Observable;

import java.io.IOException;

/**
 * @author wenchao.meng
 *
 * May 31, 2016
 */
public interface ReplicationStoreManager  extends Destroyable, Observable, Lifecycle{
	
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

	ReplId getReplId();

	/**
	 * Close current store and clear the in-memory reference only (PREPARE lease release).
	 * Does <b>not</b> destroy files or change {@code latest.store.dir}.
	 * Invoked by {@code doStop} / {@code doDispose}.
	 */
	void releaseCurrentStore() throws IOException;

}
