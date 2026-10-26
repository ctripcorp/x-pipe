package com.ctrip.xpipe.redis.keeper.store;

import com.ctrip.xpipe.redis.core.entity.GcOrphanStoreResult;
import com.ctrip.xpipe.redis.core.store.ReplId;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystemHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/** BM: the store directory is private to this keeper, so it is removed directly. */
public final class DirectStoreDeleter implements ReplicationStoreDeleter {

    private static final Logger logger = LoggerFactory.getLogger(DirectStoreDeleter.class);

    private final AsyncFileSystem fs;

    public DirectStoreDeleter(AsyncFileSystem fs) {
        this.fs = fs;
    }

    @Override
    public GcOrphanStoreResult delete(File baseDir, ReplId replId) {
        try {
            AsyncFileSystemHelper.await(() -> fs.rmdir(baseDir.getAbsolutePath(), true), "rmdir store " + baseDir);
            return GcOrphanStoreResult.deleted(replId.id());
        } catch (Throwable th) {
            logger.warn("[delete][rmdir failed]{}", baseDir, th);
            return GcOrphanStoreResult.fail(replId.id(), "rmdir failed: " + th.getMessage());
        }
    }
}
