package com.ctrip.xpipe.redis.keeper.store;

import com.ctrip.xpipe.redis.core.entity.GcOrphanStoreResult;
import com.ctrip.xpipe.redis.core.store.ReplId;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFile;

import java.io.File;

/** Deletes a released store directory; used by destroy and by the orphan-store GC. */
public interface ReplicationStoreDeleter {
    String META_FILE = "store_manager_meta.properties";

    /** Orphan-store GC: no handle, opens meta itself. */
    GcOrphanStoreResult delete(File baseDir, ReplId replId);

    /**
     * destroy: {@code heldMeta} is the manager's still-open meta write handle (null if none). Never opens
     * meta; ownership of the handle moves to the deleter, which always closes it.
     */
    GcOrphanStoreResult delete(File baseDir, ReplId replId, AsyncFile heldMeta);
}
