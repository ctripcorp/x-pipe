/**
 * 
 */
package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;

import java.util.concurrent.ScheduledExecutorService;

/**
 * @author marsqing
 *
 *         Jul 21, 2016 5:29:44 PM
 */
public class RdbOnlyPsync extends AbstractReplicationStorePsync{

	public RdbOnlyPsync(SimpleObjectPool<NettyClient> clientPool, ReplicationStore store, boolean supportGtid, ScheduledExecutorService scheduled) {
		super(clientPool, false, supportGtid, scheduled);
		currentReplicationStore = store;
	}

	@Override
	protected ReplicationStore getCurrentReplicationStore() {
		return currentReplicationStore;
	}

	@Override
	protected void doWhenFullSyncToNonFreshReplicationStore(String masterRunid) {
		// always a fresh ReplicationStore, so do nothing
	}

	@Override
	protected boolean useKeeperPsync() {
		return false;
	}

}
