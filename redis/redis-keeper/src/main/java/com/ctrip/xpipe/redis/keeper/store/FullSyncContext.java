package com.ctrip.xpipe.redis.keeper.store;

import com.ctrip.xpipe.redis.core.store.RdbStore;

/**
 * @author marsqing
 *
 *         Aug 4, 2016 1:50:03 PM
 */
public class FullSyncContext {

	private boolean fullSyncPossible;

	private RdbStore rdbStore;

	public FullSyncContext(boolean fullSyncPossible) {
		this.fullSyncPossible = fullSyncPossible;
	}

	public FullSyncContext(boolean fullSyncPossible, RdbStore rdbStore) {
		this.fullSyncPossible = fullSyncPossible;
		this.rdbStore = rdbStore;
	}

	public boolean isFullSyncPossible() {
		return fullSyncPossible;
	}

	public RdbStore getRdbStore() {
		return rdbStore;
	}
}
