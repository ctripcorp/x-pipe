package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;

import java.util.concurrent.ScheduledExecutorService;

public class RdbOnlyGapAllowedSync extends AbstractReplicationStoreGapAllowedSync{

	public RdbOnlyGapAllowedSync(SimpleObjectPool<NettyClient> clientPool, ReplicationStore store, ScheduledExecutorService scheduled) {
		super(clientPool, false, scheduled, DEFAULT_XSYNC_MAXGAP);
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

	protected void failPsync(Throwable throwable) {
		super.failPsync(throwable);
		try {
			getLogger().info("[failPsync][release rdb]");
			currentReplicationStore.close();
			currentReplicationStore.destroy();
		} catch (Throwable th) {
			getLogger().warn("[failPsync][release rdb] fail", th);
		}
	}

}
