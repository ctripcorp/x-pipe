package com.ctrip.xpipe.redis.keeper.store;

import com.ctrip.xpipe.redis.core.entity.GcOrphanStoreResult;
import com.ctrip.xpipe.redis.core.store.ReplId;

import java.io.File;

/** Deletes a released store directory; used by destroy and by the orphan-store GC. */
public interface ReplicationStoreDeleter {
    String META_FILE = "store_manager_meta.properties";

    GcOrphanStoreResult delete(File baseDir, ReplId replId);
}
