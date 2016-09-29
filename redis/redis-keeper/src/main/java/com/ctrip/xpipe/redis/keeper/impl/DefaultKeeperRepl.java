package com.ctrip.xpipe.redis.keeper.impl;

import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.keeper.KeeperRepl;

/**
 * @author wenchao.meng
 *
 *         May 23, 2016
 */
public class DefaultKeeperRepl implements KeeperRepl {

	private ReplicationStore replicationStore;

	public DefaultKeeperRepl(ReplicationStore replicationStore) {

		this.replicationStore = replicationStore;
	}

	@Override
	public long getKeeperBeginOffset() {
		return replicationStore.getMetaStore().getKeeperBeginOffset();
	}

	@Override
	public long getKeeperEndOffset() {
		return replicationStore.getKeeperEndOffset();
	}
}
