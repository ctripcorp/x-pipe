/**
 * 
 */
package com.ctrip.xpipe.redis.keeper.store;

/**
 * @author marsqing
 *
 *         Aug 4, 2016 1:50:03 PM
 */
public class FullSyncContext {

	private boolean fullSyncPossible;

	private DefaultRdbStore rdbStore;

	public FullSyncContext(boolean fullSyncPossible) {
		this.fullSyncPossible = fullSyncPossible;
	}

	public FullSyncContext(boolean fullSyncPossible, DefaultRdbStore rdbStore) {
		this.fullSyncPossible = fullSyncPossible;
		this.rdbStore = rdbStore;
	}

	public boolean isFullSyncPossible() {
		return fullSyncPossible;
	}

	public DefaultRdbStore getRdbStore() {
		return rdbStore;
	}
}
