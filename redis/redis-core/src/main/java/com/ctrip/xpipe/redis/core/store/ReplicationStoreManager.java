package com.ctrip.xpipe.redis.core.store;

import com.ctrip.xpipe.api.lifecycle.Destroyable;
import com.ctrip.xpipe.api.lifecycle.Lifecycle;
import com.ctrip.xpipe.api.observer.Observable;

import java.io.File;
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

	/**
	 * Switch this manager between production and read-only mode.
	 * Only legal when the manager is <b>not</b> started; otherwise {@link IllegalStateException}.
	 */
	void setReadOnly(boolean readOnly);

	boolean isReadOnly();

	/**
	 * Non-blocking accessor for the already-opened store.
	 * No lock, no FS call; returns {@code null} if none is open.
	 * Intended for Redis command threads (D10).
	 */
	ReplicationStore getOpenedStore();

	/**
	 * Manager base directory ({@code {keeperBase}/{replId}}). Available after initialize.
	 */
	File getBaseDir();

	/**
	 * Re-read {@code store_manager_meta.properties} from disk (drop cache, force load).
	 * Returns {@code latest.store.dir} or {@code null}. Does not open or close the current store.
	 * Used by PrepareStoreWatcher (D8) so occupying-keeper 换店 is visible.
	 * <p>
	 * Callers must have closed the handle via {@link #closeReadOnlyMetaHandle()} beforehand (D48):
	 * this method no longer closes it, so the reopen happens after the watcher's quiet window.
	 */
	String reloadLatestStoreDir() throws IOException;
	/**
	 * Close the manager-meta handle in read-only mode (PrepareStoreWatcher closed phase, D48).
	 * Idempotent, no-op in production mode. Keeps the in-memory {@code latest.store.dir} cache so
	 * request-side accessors stay FS-free while the handle is closed.
	 */
	void closeReadOnlyMetaHandle();

	/**
	 * Mark the store dir as shared by several keepers (TFS). A shared store may only be opened for
	 * write, created, gc'ed or destroyed by the keeper that MetaServer made the slot holder; see
	 * {@link #setStoreWriteOwner(boolean)}. Must be set before the manager is used. Default: private.
	 */
	default void setSharedStore(boolean sharedStore) {
	}

	/**
	 * Grant / revoke write ownership of a shared store. No-op for a private store, which is always
	 * owned by its only keeper. Revoking does not close an opened store: lease release still goes
	 * through {@link #releaseCurrentStore()}.
	 */
	default void setStoreWriteOwner(boolean owner) {
	}

	default boolean isStoreWriteOwner() {
		return true;
	}

}
