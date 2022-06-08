package com.ctrip.xpipe.redis.keeper.store;

import com.ctrip.xpipe.redis.core.store.FULLSYNC_FAIL_CAUSE;
import com.ctrip.xpipe.redis.core.store.RdbStore;

/**
 * @author marsqing
 *
 *         Aug 4, 2016 1:50:03 PM
 */
public class FullSyncContext {

	private boolean fullSyncPossible;

	private RdbStore rdbStore;

	private FULLSYNC_FAIL_CAUSE cause;

	public FullSyncContext(boolean fullSyncPossible, FULLSYNC_FAIL_CAUSE cause) {
		this.fullSyncPossible = fullSyncPossible;
		this.cause = cause;
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

	public FULLSYNC_FAIL_CAUSE getCause() {
		return cause;
	}

}
